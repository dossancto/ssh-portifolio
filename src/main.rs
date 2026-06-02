use std::collections::HashMap;
use std::sync::Arc;

use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Alignment, Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::widgets::{Block, Borders, Clear, Paragraph};
use ratatui::{Terminal, TerminalOptions, Viewport};
use russh::keys::ssh_key::{self, PublicKey};
use russh::server::*;
use russh::{Channel, ChannelId, Pty};
use tokio::sync::Mutex;
use tokio::sync::mpsc::{UnboundedSender, unbounded_channel};

type SshTerminal = Terminal<CrosstermBackend<TerminalHandle>>;

struct App {
    pub counter: usize,
}

impl App {
    pub fn new() -> App {
        Self { counter: 0 }
    }
}

struct TerminalHandle {
    sender: UnboundedSender<Vec<u8>>,
    // The sink collects the data which is finally sent to sender.
    sink: Vec<u8>,
}

impl TerminalHandle {
    async fn start(handle: Handle, channel_id: ChannelId) -> Self {
        let (sender, mut receiver) = unbounded_channel::<Vec<u8>>();
        tokio::spawn(async move {
            while let Some(data) = receiver.recv().await {
                let result = handle.data(channel_id, data).await;
                if result.is_err() {
                    eprintln!("Failed to send data: {result:?}");
                }
            }
        });
        Self {
            sender,
            sink: Vec::new(),
        }
    }
}

// The crossterm backend writes to the terminal handle.
impl std::io::Write for TerminalHandle {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.sink.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        let result = self.sender.send(self.sink.clone());
        if result.is_err() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                result.unwrap_err(),
            ));
        }

        self.sink.clear();
        Ok(())
    }
}

#[derive(Clone)]
struct AppServer {
    clients: Arc<Mutex<HashMap<usize, (SshTerminal, App)>>>,
    id: usize,
}

impl AppServer {
    pub fn new() -> Self {
        Self {
            clients: Arc::new(Mutex::new(HashMap::new())),
            id: 0,
        }
    }

    pub async fn run(&mut self) -> Result<(), anyhow::Error> {
        let clients = self.clients.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

                for (_, (terminal, app)) in clients.lock().await.iter_mut() {
                    app.counter += 1;

                    terminal
                        .draw(|f| {
                            let area = f.area();
                            f.render_widget(Clear, area);
                            let accent = match app.counter % 4 {
                                _ => Color::LightGreen,
                            };

                            let frame = Block::default()
                                .title(" dossancto ")
                                .title_alignment(Alignment::Center)
                                .borders(Borders::ALL)
                                .border_style(Style::default().fg(Color::DarkGray));
                            let inner = frame.inner(area);
                            f.render_widget(frame, area);

                            let sections = Layout::default()
                                .direction(Direction::Vertical)
                                .constraints([
                                    Constraint::Length(2),
                                    Constraint::Length(8),
                                    Constraint::Length(12),
                                    Constraint::Length(3),
                                ])
                                .split(inner);

                            let title = Paragraph::new(
                                r"
 _____     ______     ______     ______     ______     __   __     ______     ______   ______    
/\  __-.  /\  __ \   /\  ___\   /\  ___\   /\  __ \   /\ '-.\ \   /\  ___\   /\__  _\ /\  __ \   
\ \ \/\ \ \ \ \/\ \  \ \___  \  \ \___  \  \ \  __ \  \ \ \-.  \  \ \ \____  \/_/\ \/ \ \ \/\ \  
 \ \____-  \ \_____\  \/\_____\  \/\_____\  \ \_\ \_\  \ \_\\'\_\  \ \_____\    \ \_\  \ \_____\ 
  \/____/   \/_____/   \/_____/   \/_____/   \/_/\/_/   \/_/ \/_/   \/_____/     \/_/   \/_____/ 
                                                                                                 
",
                            )
                            .alignment(Alignment::Center)
                            .style(Style::default().fg(accent).add_modifier(Modifier::BOLD));

                            let roletitle = Paragraph::new(
                                r"
                                                       
 ____  _____ _____ _____ __    _____ _____ _____ _____ 
|    \|   __|  |  |   __|  |  |     |  _  |   __| __  |
|  |  |   __|  |  |   __|  |__|  |  |   __|   __|    -|
|____/|_____|\___/|_____|_____|_____|__|  |_____|__|__|
                                                       
",
                            )
                            .alignment(Alignment::Center)
                            .style(Style::default().fg(Color::LightBlue).add_modifier(Modifier::BOLD));

                            let subtitle = Paragraph::new(
                                "Building fast, reliable, and creative software experiences.",
                            )
                            .alignment(Alignment::Center)
                            .style(Style::default().fg(Color::Gray));

                            let controls = Paragraph::new("Press 'q' to quit")
                                .alignment(Alignment::Center)
                                .style(Style::default().fg(Color::DarkGray));

                            f.render_widget(subtitle, sections[0]);
                            f.render_widget(title, sections[1]);
                            f.render_widget(roletitle, sections[2]);
                            f.render_widget(controls, sections[3]);
                        })
                        .unwrap();
                }
            }
        });

        let config = Config {
            inactivity_timeout: Some(std::time::Duration::from_secs(3600)),
            auth_rejection_time: std::time::Duration::from_secs(3),
            auth_rejection_time_initial: Some(std::time::Duration::from_secs(0)),
            keys: vec![
                russh::keys::PrivateKey::random(&mut rand::rng(), ssh_key::Algorithm::Ed25519)
                    .unwrap(),
            ],
            nodelay: true,
            ..Default::default()
        };

        self.run_on_address(Arc::new(config), ("0.0.0.0", 2222))
            .await?;
        Ok(())
    }
}

impl Server for AppServer {
    type Handler = Self;
    fn new_client(&mut self, _: Option<std::net::SocketAddr>) -> Self {
        let s = self.clone();
        self.id += 1;
        s
    }
}

impl Handler for AppServer {
    type Error = anyhow::Error;

    async fn channel_open_session(
        &mut self,
        channel: Channel<Msg>,
        session: &mut Session,
    ) -> Result<bool, Self::Error> {
        let terminal_handle = TerminalHandle::start(session.handle(), channel.id()).await;

        let backend = CrosstermBackend::new(terminal_handle);

        // the correct viewport area will be set when the client request a pty
        let options = TerminalOptions {
            viewport: Viewport::Fixed(Rect::default()),
        };

        let terminal = Terminal::with_options(backend, options)?;
        let app = App::new();

        let mut clients = self.clients.lock().await;
        clients.insert(self.id, (terminal, app));

        Ok(true)
    }

    async fn auth_publickey(&mut self, _: &str, _: &PublicKey) -> Result<Auth, Self::Error> {
        Ok(Auth::Accept)
    }

    async fn data(
        &mut self,
        channel: ChannelId,
        data: &[u8],
        session: &mut Session,
    ) -> Result<(), Self::Error> {
        match data {
            // Pressing 'q' closes the connection.
            b"q" => {
                self.clients.lock().await.remove(&self.id);
                session.close(channel)?;
            }
            // Pressing 'c' resets the counter for the app.
            // Only the client with the id sees the counter reset.
            b"c" => {
                let mut clients = self.clients.lock().await;
                let (_, app) = clients.get_mut(&self.id).unwrap();
                app.counter = 0;
            }
            _ => {}
        }

        Ok(())
    }

    /// The client's window size has changed.
    async fn window_change_request(
        &mut self,
        _: ChannelId,
        col_width: u32,
        row_height: u32,
        _: u32,
        _: u32,
        _: &mut Session,
    ) -> Result<(), Self::Error> {
        let rect = Rect {
            x: 0,
            y: 0,
            width: col_width as u16,
            height: row_height as u16,
        };

        let mut clients = self.clients.lock().await;
        let (terminal, _) = clients.get_mut(&self.id).unwrap();
        terminal.resize(rect)?;

        Ok(())
    }

    /// The client requests a pseudo-terminal with the given
    /// specifications.
    ///
    /// **Note:** Success or failure should be communicated to the client by calling
    /// `session.channel_success(channel)` or `session.channel_failure(channel)` respectively.
    async fn pty_request(
        &mut self,
        channel: ChannelId,
        _: &str,
        col_width: u32,
        row_height: u32,
        _: u32,
        _: u32,
        _: &[(Pty, u32)],
        session: &mut Session,
    ) -> Result<(), Self::Error> {
        let rect = Rect {
            x: 0,
            y: 0,
            width: col_width as u16,
            height: row_height as u16,
        };

        let mut clients = self.clients.lock().await;
        let (terminal, _) = clients.get_mut(&self.id).unwrap();
        terminal.resize(rect)?;

        session.channel_success(channel)?;

        Ok(())
    }
}

impl Drop for AppServer {
    fn drop(&mut self) {
        let id = self.id;
        let clients = self.clients.clone();
        tokio::spawn(async move {
            let mut clients = clients.lock().await;
            clients.remove(&id);
        });
    }
}

#[tokio::main]
async fn main() {
    let mut server = AppServer::new();
    server.run().await.expect("Failed running server");
}

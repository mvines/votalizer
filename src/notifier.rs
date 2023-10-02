use {reqwest::Client, serde_json::json, std::env};

pub enum Config {
    Slack { webhook: String },
    Discord { webhook: String, username: String },
}

pub struct Notifier {
    client: Client,
    configs: Vec<Config>,
}

impl Notifier {
    pub fn default() -> Self {
        let mut configs = vec![];
        if let Ok(webhook) = env::var("SLACK_WEBHOOK") {
            configs.push(Config::Slack { webhook });
        }
        if let Ok(webhook) = env::var("DISCORD_WEBHOOK") {
            configs.push(Config::Discord {
                webhook,
                username: env::var("DISCORD_USERNAME").unwrap_or("votalizer".to_string()),
            })
        }
        Notifier {
            client: Client::new(),
            configs,
        }
    }

    pub async fn send(&self, msg: &str) {
        for config in &self.configs {
            let (webhook, data, service_name) = match config {
                Config::Slack { webhook } => (webhook, json!({ "text": msg }), "Slack"),
                Config::Discord { webhook, username } => (
                    webhook,
                    json!({ "username": username, "content": msg }),
                    "Discord",
                ),
            };

            if let Err(err) = self.client.post(webhook).json(&data).send().await {
                eprintln!("Failed to send {service_name} message: {:?}", err);
            }
        }
    }
}

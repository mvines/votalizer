# Votalizer

The votalizer uses the [Solana RPC PubSub WebSocket Endpoint](https://docs.solana.com/developing/clients/jsonrpc-api)
to monitor validator votes in real time.

If a lockout violation is detected, an incident log file is created with
details.

Note that some of the transaction signatures in an incident log file may not be
available on http://explorer.solana.com. This occurs specifically when a
transaction lands in a non-finalized fork.

To be notified by Slack when an incident occurs, export your desired Slack
webhook to the environment before running the votalizer:
```
export SLACK_WEBHOOK=https://hooks.slack.com/services/...
```

To be notified by Discord when an incident occurs, export your desired Discord
webhook to the environment before running the votalizer:
```
export DISCORD_WEBHOOK=https://discord.com/api/webhooks/...
export DISCORD_USERNAME=username_to_show_in_message
```

### RPC Node Requirements

The RPC node used by the votalizer must be configured with the `--full-rpc-api`
and `--rpc-pubsub-enable-vote-subscription` flags.

It's easiest if you just run the `votalizer` on the same host as your RPC node,
and by default the `votalizer` will already attempt to connect to `localhost`.

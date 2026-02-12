# Review Subscription Guide for elastic/elasticsearch

This guide explains how to subscribe to code reviews in the elastic/elasticsearch repository.

## For Contributors

### Method 1: Watch the Repository

The simplest way to receive notifications about all pull requests:

```bash
# Using GitHub CLI
gh repo set-default elastic/elasticsearch
gh repo subscribe
```

Or visit https://github.com/elastic/elasticsearch and click "Watch" → "All Activity" or "Pull requests only".

### Method 2: Team-Based Subscriptions (CODEOWNERS)

The repository uses `.github/CODEOWNERS` to automatically request reviews from teams when their code areas are modified.

Teams currently configured:
- `@elastic/es-core-infra` - Core infrastructure
- `@elastic/es-delivery` - Build and delivery
- `@elastic/es-security` - Security components
- `@elastic/stack-monitoring` - Monitoring
- `@elastic/fleet` - Fleet components

If you're a member of these teams, you'll automatically be notified when PRs touch your areas.

### Method 3: Subscribe to Specific Pull Requests

For individual PRs you're interested in:

```bash
# Using GitHub CLI
gh pr view <number> --web
# Then click "Subscribe" in the web UI
```

Or visit the PR page and click the "Subscribe" button in the sidebar.

### Method 4: Email Filters

Set up email filters to organize review notifications:

- **Subject filter**: `[elastic/elasticsearch]`
- **Label filter**: `review_requested` or `review_required`
- **Team mentions**: `@elastic/<team-name>`

## For Team Maintainers

### Adding Team to CODEOWNERS

To add your team to automatic review requests:

1. Edit `.github/CODEOWNERS`
2. Add your team and file patterns:
   ```
   path/to/files/** @elastic/your-team
   ```
3. Submit a PR with your changes

### Best Practices

1. **Be Specific**: Use precise file patterns to avoid notification overload
2. **Update Regularly**: Keep CODEOWNERS in sync with team responsibilities
3. **Use Teams**: Prefer team mentions over individual users for sustainability

## Notification Settings

### GitHub Notification Preferences

Configure at: https://github.com/settings/notifications

Recommended settings for active reviewers:
- **Participating**: On
- **Watching**: On (for repos you maintain)
- **Email**: Custom (configure filters)
- **Web + Mobile**: On (for urgent reviews)

### Slack/Chat Integration

Many teams use bots to forward GitHub notifications to Slack. Check with your team lead for integration details.

## Troubleshooting

### Not Receiving Review Requests?

1. Check you're a member of the relevant team
2. Verify the team has write access to the repository
3. Ensure your GitHub notification settings are enabled
4. Check if CODEOWNERS file is in the base branch

### Too Many Notifications?

1. Unwatch the repository (you'll still get direct mentions)
2. Use more specific file patterns in CODEOWNERS
3. Set up email filters to organize notifications
4. Use "Custom" watch settings to select specific events

## Additional Resources

- [GitHub CODEOWNERS Documentation](https://docs.github.com/en/repositories/managing-your-repositorys-settings-and-features/customizing-your-repository/about-code-owners)
- [Managing Notifications](https://docs.github.com/en/account-and-profile/managing-subscriptions-and-notifications-on-github/setting-up-notifications/configuring-notifications)
- [Elasticsearch Contributing Guide](./CONTRIBUTING.md)

---

For questions about review subscriptions, please contact the repository maintainers or your team lead.

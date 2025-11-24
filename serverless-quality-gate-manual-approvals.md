# Serverless Quality Gate Manual Approvals

This document contains all serverless quality gate checks that required manual approval from the last 20 promotions.

## Manual Approvals Table

| Date | Environment | Failure/Issue Details | Manual Approval Message | Buildkite Link |
|------|-------------|----------------------|------------------------|----------------|
| 2025-11-10 | staging | SLO issues | transient 429s | [Build #319](https://buildkite.com/elastic/elasticsearch-serverless-promote-release/builds/319) |
| 2025-11-10 | qa | Project SLO issues | Project of SLO recovered we need to revisit our SLO strategy for promotions here I think to make this less distracting but still reliable | [Build #319](https://buildkite.com/elastic/elasticsearch-serverless-promote-release/builds/319) |
| 2025-11-05 | staging | Project issues | https://elastic.slack.com/archives/C0631EPCFLP/p1762350012524689 | [Build #318](https://buildkite.com/elastic/elasticsearch-serverless-promote-release/builds/318) |
| 2025-11-05 | qa | Project SLO issues | As discussed in https://elastic.slack.com/archives/C05PJK7UZE1/p1762346054878269?thread_ts=1762283421.843679&cid=C05PJK7UZE1 we ignore the SLO for project cc5b2b7dab2b4b7b98a3a3eae71e8f98 as it's recuperating slowly | [Build #318](https://buildkite.com/elastic/elasticsearch-serverless-promote-release/builds/318) |
| 2025-10-27 | qa | SLO threshold issues | https://elastic.slack.com/archives/C05PJK7UZE1/p1761570514143299?thread_ts=1761566557.196319&cid=C05PJK7UZE1 | [Build #316](https://buildkite.com/elastic/elasticsearch-serverless-promote-release/builds/316) |
| 2025-10-20 | qa | Project health issues | Issue with troublesome project has been resolved. See https://elastic.slack.com/archives/C09NB1LNEPJ | [Build #315](https://buildkite.com/elastic/elasticsearch-serverless-promote-release/builds/315) |
| 2025-10-16 | production-canary | Buildkite agent stopped | Buildkite agent stopped with soft fail after waiting properly for 24h. All quality gates checks passed. | [Build #314](https://buildkite.com/elastic/elasticsearch-serverless-promote-release/builds/314) |
| 2025-10-15 | staging | Projects no longer exist | The three projects no longer exist and seemed in the process of shutting down - see https://elastic.slack.com/archives/C05PJK7UZE1/p1760529019169529?thread_ts=1760528319.123829&cid=C05PJK7UZE1 | [Build #314](https://buildkite.com/elastic/elasticsearch-serverless-promote-release/builds/314) |
| 2025-10-15 | qa | Red health cluster | Checked red health was due to https://elasticco.atlassian.net/browse/ES-13187 - check https://elastic.slack.com/archives/C05PJK7UZE1/p1760516114291399?thread_ts=1760458971.389209&cid=C05PJK7UZE1 | [Build #314](https://buildkite.com/elastic/elasticsearch-serverless-promote-release/builds/314) |
| 2025-10-08 | staging | E2E test setup issues | e2e tests failed due to setup issue with rotated project api keys for staging | [Build #313](https://buildkite.com/elastic/elasticsearch-serverless-promote-release/builds/313) |
| 2025-10-08 | qa | Red health threshold exceeded | The red health percentage was 0.152, just over the threshold of 0.15. I have looked at the dashboards and nothing looks concerning. | [Build #313](https://buildkite.com/elastic/elasticsearch-serverless-promote-release/builds/313) |
| 2025-10-02 | staging | Transient issues | Retry for https://elastic.slack.com/archives/C09GW9DPXA6/p1759428637467299 | [Build #312](https://buildkite.com/elastic/elasticsearch-serverless-promote-release/builds/312) |
| 2025-09-25 | production-canary | Error log rate with NPE | The error log rate is caused by NPE exceptions. Considering this as temporary issue because metrics are attempting to fetching cluster stats too early, when initial cluster state was not set yet. - https://elastic.slack.com/archives/C09GG7A3CQ7/p1758790369294919 - https://elasticco.atlassian.net/browse/ES-13022 | [Build #36 (Emergency)](https://buildkite.com/elastic/elasticsearch-serverless-promote-emergency-release/builds/36) |
| 2025-09-25 | staging | Manual verification needed | Staging verified manually. See https://elastic.slack.com/archives/C05PJK7UZE1/p1758785399579619?thread_ts=1758783527.000659&cid=C05PJK7UZE1 | [Build #36 (Emergency)](https://buildkite.com/elastic/elasticsearch-serverless-promote-emergency-release/builds/36) |
| 2025-09-25 | staging | Quality gate issues | More details here: https://elastic.slack.com/archives/C05PJK7UZE1/p1758784646185769?thread_ts=1758783527.000659&cid=C05PJK7UZE1 | [Build #36 (Emergency)](https://buildkite.com/elastic/elasticsearch-serverless-promote-emergency-release/builds/36) |
| 2025-09-16 | staging | Known alert fired | known "Elasticsearch Search 429 Rate burn rate" alert fired | [Build #310](https://buildkite.com/elastic/elasticsearch-serverless-promote-release/builds/310) |
| 2025-09-16 | qa | High red health rate | the high red health rate is something general we dealing with in QA these days. It hasn't raised really by this promotion. Also error logs kept been low | [Build #310](https://buildkite.com/elastic/elasticsearch-serverless-promote-release/builds/310) |
| 2025-09-08 | staging | Transient 429 alerts | Transient issues and known 429 alerts | [Build #308](https://buildkite.com/elastic/elasticsearch-serverless-promote-release/builds/308) |
| 2025-09-01 | staging | RED health after restarts | There were 6 instances where health was reported as RED. All occurred just after restarts due to shard unavailability, and all projects recovered themselves within a short period. We decided this is not a blocker for the promotion, so I'm restarting it | [Build #307](https://buildkite.com/elastic/elasticsearch-serverless-promote-release/builds/307) |
| 2025-08-27 | staging | Transient 429 errors | A few clusters experienced a momentary high rate of 429 errors that tripped alerts. They all auto resolved themselves shortly after alerting. These alerts aren't considered fine tuned yet and have been known to be spurious sometimes. Proceeding with the release. | [Build #306](https://buildkite.com/elastic/elasticsearch-serverless-promote-release/builds/306) |
| 2025-08-27 | qa | Pre-existing indices bug | The two projects (b2289f7bd808419abd85edeed7be1cce & c0b90ee5bb90441995ad9faf8b6b6c3d) that failed are now green. The source of the issue was a bug with pre-existing indices. The promotion changes should clean up the problem for new indices, but there could be some projects alerts until indices roll over. It has been agreed upon that the promotion should proceed. | [Build #306](https://buildkite.com/elastic/elasticsearch-serverless-promote-release/builds/306) |
| 2025-08-18 | staging | Failed alerts triaged | Triaged failed alerts in https://elastic.slack.com/archives/C05PJK7UZE1/p1755535840885439. We've determined that both alerts are not blocking. | [Build #304](https://buildkite.com/elastic/elasticsearch-serverless-promote-release/builds/304) |
| 2025-08-18 | qa | Red projects spike | Checked the overview dashboard for red projects which are now below the spike we had due to the promotion | [Build #304](https://buildkite.com/elastic/elasticsearch-serverless-promote-release/builds/304) |
| 2025-08-12 | staging | Transient alerts | Transient Alerts have been investigated manually. Discussion is slack at https://elastic.slack.com/archives/C05PJK7UZE1/p1754997331769909 | [Build #303](https://buildkite.com/elastic/elasticsearch-serverless-promote-release/builds/303) |
| 2025-08-12 | qa | Kibana FTR incompatibility | Incompatibility issue with Kibana FTR Tests. Fixed by the kibana team and validated by rerunning faliing tests. now all green | [Build #303](https://buildkite.com/elastic/elasticsearch-serverless-promote-release/builds/303) |
| 2025-08-05 | staging | Known serverless alerts | Two alerts are already known about in Serverless | [Build #302](https://buildkite.com/elastic/elasticsearch-serverless-promote-release/builds/302) |

## Summary

- **Total manual approvals**: 26 across the last 20 promotions
- **Date range**: August 2025 - November 2025

### Most Common Issues

1. **Transient 429 rate errors**: 5 occurrences
2. **Red health status / SLO violations**: 7 occurrences
3. **Known alerts or platform issues**: 4 occurrences
4. **Test failures or setup issues**: 4 occurrences
5. **Project-specific issues**: 6 occurrences

### Environment Breakdown

- **qa** and **staging** environments required the most manual interventions
- Most manual approvals were for transient issues that self-resolved or were deemed non-blocking
- Only 1 emergency promotion required manual approval (Build #36 on 2025-09-25)

## Notes

- This data is based on the last 20 serverless promotions
- Manual approvals indicate cases where automated quality gates flagged issues requiring human judgment
- Most issues were determined to be non-blocking after investigation

## 9.3.0 [elasticsearch-9.3.0-known-issues]

**Vector search**

::::{dropdown} Add known issue for bbq_disk license check.
The bbq_disk index format, licensed under the Enterprise subscription tier was introduced in 9.2.0 without appropriate license enforcement. This allowed users to create and use bbq_disk indices without an Enterprise license. This will be addressed in version 9.3.0. Upon upgrading to 9.3+ indices created on 9.2 will still be available for updates and queries, but new indices created on 9.3+ will require an Enterprise license.

For more information, check [#139011](https://github.com/elastic/elasticsearch/pull/139011).

% **Impact**<br>_Add a description of the impact_

% **Action**<br>_Add a description of the what action to take_
::::

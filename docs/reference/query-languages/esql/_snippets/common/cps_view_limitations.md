* You can define views in both origin and linked projects.
* CPS resolves index expressions against indices in every project that the
  expression targets, but it resolves views only in the origin project. Views
  in linked projects are invisible to the query, including through qualified
  expressions such as `FROM linked-project:view`.
* CPS uses the same resolution process for index expressions in the top-level
  query and inside view definitions.
* An unqualified index expression can match a view in the origin project and
  indices in linked projects. CPS returns results from both.

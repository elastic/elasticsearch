* You can define views in both origin and linked projects.
* CPS resolves index expressions against both indices and views in every project
  that the expression targets. CPS uses the same process for index expressions
  in the top-level query and inside view definitions.
* An unqualified index expression can match a view in the origin project and
  indices in linked projects. CPS returns results from both. However, the query
  fails if the expression also matches a view in a linked project, because CPS
  cannot query views in linked projects.

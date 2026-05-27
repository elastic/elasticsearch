#### Branching inside views

Commands that also generate branched query plans
(`FORK`, [subqueries](/reference/query-languages/esql/esql-subquery.md))
are usually not allowed within a view definition, unless the branch points
can be merged via
[query compaction](/reference/query-languages/esql/esql-views.md#query-compaction).

Views may be nested up to depth 10. Branching and nesting are allowed in
combination as long as there is never more than one branch point:

* A view can contain subqueries, but that view cannot be used together with
  other views, and the subqueries can only reference nested views that contain
  no further branching.
* A subquery can contain views, but those views must not introduce any
  additional branch points via subqueries or `FORK`.

#### Cross-cluster and serverless

* [Cross-cluster search](/reference/query-languages/esql/esql-cross-clusters.md):
    * Remote views in CCS are not allowed (ie. `FROM cluster:view` will only
      match remote indexes with the name `view`. If a remote view is found,
      the query will fail).
    * If a remote index matches a local view name, the query will fail.
* Serverless and Cross-project search:
    * Views are initially unavailable in serverless.

#### Query parameters

Query parameters are not allowed in the view definition, and therefore query
parameters in the main query will never impact the view results.

#### Known issues (tech preview)

Views are in tech-preview and there are a number of known issues, or behavior
that is likely to change in the future:

* Query DSL filtering on the main query will currently affect the source
  indices in the view definition, and this will change in later releases.
    * The future design will have the query filtering impact the output of the
      view, not the source indices.
* `METADATA` directives inside and outside a view definition behave the same
  as they do for
  [`METADATA` in subqueries](/reference/query-languages/esql/esql-subquery.md#subqueries-with-metadata).
  This will change for views.

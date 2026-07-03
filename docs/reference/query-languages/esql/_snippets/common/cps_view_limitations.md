 * Remote views are not supported. A query will fail if its pattern (ie `FROM remote:name`) matches any remote views.
 * The same applies to flat expressions (ie `FROM name`), if the name is resolved to a view on any of the linked projects, the query will fail.
 * Unlike in CCS, if a local view name does not match a remote view name, but does match a remote index name,
   the query will succeed and produce results from both the local view and the remote index.
 * If a local view definition contains index patterns that match local and remote indexes,
   data from both local and remote indexes will be returned.
   That is to say both the view name and the index patterns inside the view definition
   conform to the same index resolution rules as any other index pattern in a normal {{esql}} query.

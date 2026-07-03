 * Views may be defined only in the origin project. If a source expression in the top-level query or a nested view resolves to a view in a linked project, the query fails.
 * An unqualified index expression can resolve to both a view in the origin project and indices in linked projects. Results from both sources are returned.
 * Index expressions in an origin-project view definition follow the standard CPS index-resolution rules and can resolve to indices in the origin and linked projects.

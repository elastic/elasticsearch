# ES|QL core

This project originated as a copy of the `ql` x-pack plugin.
It contains some fundamental classes used in `esql`, like `Node`, its subclasses `Expression`, `QueryPlan`, and the plan optimizer code.
Originally, `ql` shared classes between ES|QL, SQL and EQL, but ES|QL diverged far enough to justify a split.

## Warning

- **Consider the contents of this project untested.**
  There may be some tests in `sql` and `eql` that may have indirectly covered the initial version of this (when it was copied from `ql`);
  but neither do these tests apply to `esql`, nor do they even run against this.
- **Consider this project technical debt.**
  The contents of this project need to be consolidated with the `esql` plugin.
  In particular, there is a significant amount of code (or code paths) that are not used/executed at all in `esql`.

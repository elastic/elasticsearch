/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

/**
 * This interface marks a command which does not add or remove rows, and is neutral towards limit aggregation.
 * This means that this sequence:
 *  ```
 *  ... LIMIT X | MY_COMMAND
 *  ```
 *  is safe to replace with this sequence:
 *  ```
 *  ... local LIMIT X | MY_COMMAND | LIMIT X
 *  Where the local limit is applied only on the node.
 *  ```
 *  It is not true, for example, for WHERE:
 *  ```
 *  ... LIMIT X | WHERE side="dark"
 *  ```
 *  If the first X rows do not contain any "dark" rows, the result is empty, however if we switch:
 *  ```
 *  ... local LIMIT X | WHERE side="dark" | LIMIT X
 *  ```
 *  and we have N nodes, then the first N*X rows may contain "dark" rows, and the final result is not empty in this case.
 * <br>
 * This property is important for processing Limit and TopN with remote operations such as remote ENRICH, since it allows
 * us to localize the limits without changing the semantics.
 */
public interface CardinalityPreserving {}

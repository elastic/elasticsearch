/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

/**
 * This interface marks a command which does not add or remove rows.
 *  This means these are equivalent:
 *  ```
 *  ... | LIMIT X | MY_COMMAND
 *  ```
 *  and
 *  ```
 *  ... | MY_COMMAND | LIMIT X
 *  ```
 *  It is not true, for example, for WHERE:
 *  ```
 *  ... | LIMIT X | WHERE side="dark"
 *  ```
 *  If the first X rows do not contain any "dark" rows, the result is empty, however if we switch:
 *  ```
 *  ... | WHERE side="dark" | LIMIT X
 *  ```
 *  And the dataset contains "dark" rows, we will get some results.
 */
public interface CardinalityPreserving {}

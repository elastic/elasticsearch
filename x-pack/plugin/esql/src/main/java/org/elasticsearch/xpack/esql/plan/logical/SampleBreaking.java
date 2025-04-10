/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

/**
 *  This interface is to check whether a plan breaks the random sampling context.
 *  <p>
 *
 *  Random sampling aims to correct <code>STATS</code> after them, e.g.
 *  <p>
 *  <code>
 *  | SAMPLE 0.1 | STATS SUM(value)
 *  </code>
 *  <p>
 *  gives an estimate of the sum of the values, and not 10% of the sum.
 *  <p>
 *
 *  For many commands inbetween this works fine, because they can be swapped
 *  with <code>SAMPLE</code>. For example,
 *  <p>
 *  <code>
 *  | SAMPLE 0.1 | SORT value
 *  </code>
 *  <p>
 *  <code>
 *  | SAMPLE 0.1 | WHERE value > 10
 *  </code>
 *  <p>
 *  are equivalent to
 *  <p>
 *  <code>
 *  | SORT value | SAMPLE 0.1
 *  </code>
 *  <p>
 *  <code>
 *  | WHERE value > 10 | SAMPLE 0.1
 *  </code>
 *  <p>
 *  (statistically equivalent, not necessary identical), and therefore succeeding
 *  <code>STATS</code> can be adjusted for the sample size.
 *  <p>
 *
 *  In other cases, commands cannot be swapped with <code>SAMPLE</code>, e.g.
 *  <p>
 *  <code>
 *  | SAMPLE 0.1 | MV_EXPAND value
 *  </code>
 *  <p>
 *  <code>
 *  | SAMPLE 0.1 | LIMIT 100
 *  </code>
 *  <p>
 *  In those cases, it also makes no sense to correct any succeeding <code>STATS</code>.
 *  <p>
 *
 *  As a rule of thumb, if an operator can be swapped with random sampling if it maps:
 *  <ul>
 *      <li>
 *          one row to one row (e.g. <code>DISSECT</code>, <code>DROP</code>, <code>ENRICH</code>,
 *          <code>EVAL</code>, <code>GROK</code>, <code>KEEP</code>, <code>RENAME</code>)
 *      </li>
 *      <li>
 *          one row to zero or one row (<code>WHERE</code>)
 *      </li>
 *      <li>
 *          reorders the rows (<code>SORT</code>)
 *      </li>
 *  </ul>
 *  Contrarily, it is sampling breaking (and should implement this interface) if it maps:
 *  <ul>
 *      <li>
 *          one row to many rows (<code>MV_EXPAND</code>)
 *      </li>
 *      <li>
 *          many rows to many rows (<code>LIMIT</code>, <code>STATS</code>)
 *      </li>
 *  </ul>
 *
 */
public interface SampleBreaking {}

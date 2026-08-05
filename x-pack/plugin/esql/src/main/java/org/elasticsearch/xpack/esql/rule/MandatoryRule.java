/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.rule;

/**
 * Marker interface indicating that an optimizer rule must not be included in the random candidate pool of the
 * differential test suite ({@code CsvOptimizerRuleDisabledIT}).
 *
 * <p>Semantics: <em>disabling this rule is not expected to be semantics-preserving</em>. A rule is a disable
 * candidate by default; it opts out by implementing this interface. Use this marker when disabling the rule would
 * produce different query results — for example because the rule is a structural rewrite that subsequent rules depend
 * on, or because the rule is responsible for maintaining a correctness invariant (as opposed to being a pure optional
 * optimisation).</p>
 *
 * <p>If disabling the rule produces wrong results and that is a <em>bug</em>, leave the rule <em>unmarked</em> so
 * that the suite can surface the issue and it can be filed and tracked separately (as with issue #155101). Only add
 * this marker when the change in output is expected and intentional.</p>
 *
 * <p>This marker does <em>not</em> prevent the rule from being disabled via an explicit
 * {@code disable_optimizer_rules} pragma value — it is purely a filter on the automated random candidate pool.</p>
 */
public interface MandatoryRule {}

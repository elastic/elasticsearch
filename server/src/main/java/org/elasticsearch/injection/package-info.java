/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

/**
 * Our dependency injection technologies: our bespoke injector, plus our legacy vendored version of Google Guice.
 * <h2>Usage</h2>
 * The new injector is {@link org.elasticsearch.injection.Injector}.
 * You create an instance using {@link org.elasticsearch.injection.Injector#create()},
 * call various methods like {@link org.elasticsearch.injection.Injector#addClass} to configure it,
 * then call {@link org.elasticsearch.injection.Injector#inject} to cause the constructors to be called.
 *
 * <h2>Operation</h2>
 * Injection proceeds in three phases:
 * <ol>
 *     <li>
 *         <em>Configuration</em>: the {@link org.elasticsearch.injection.Injector} captures the user's
 *         intent in the form of {@link org.elasticsearch.injection.spec.InjectionSpec} objects,
 *         one for each class.
 *     </li>
 *     <li>
 *         <em>Planning</em>: the {@link org.elasticsearch.injection.Planner} analyzes the
 *         {@link org.elasticsearch.injection.spec.InjectionSpec} objects, validates them,
 *         and generates a <em>plan</em> in the form of a list of {@link org.elasticsearch.injection.step.InjectionStep} objects.
 *     </li>
 *     <li>
 *         <em>Execution</em>: the {@link org.elasticsearch.injection.PlanInterpreter} runs
 *         the steps in the plan, in sequence, to actually instantiate the objects and pass them
 *         to each others' constructors.
 *     </li>
 * </ol>
 *
 * <h2>Google Guice</h2>
 * The older injector, based on Google Guice, is in the {@code guice} package.
 * The new injector is unrelated to Guice, and is intended to replace Guice eventually.
 */
package org.elasticsearch.injection;

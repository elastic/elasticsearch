/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

/**
 * Objects that describe the means by which an object instance is created for (or associated with) some given type.
 * <p>
 * The hierarchy is rooted at {@link org.elasticsearch.injection.spec.InjectionSpec}.
 * <p>
 * Differs from {@link org.elasticsearch.injection.step.InjectionStep InjectionStep} in that:
 *
 * <ul>
 *     <li>
 *         this describes the requirements, while <code>InjectionStep</code> describes the solution
 *     </li>
 *     <li>
 *         this is declarative, while <code>InjectionStep</code> is imperative
 *     </li>
 * </ul>
 */
package org.elasticsearch.injection.spec;

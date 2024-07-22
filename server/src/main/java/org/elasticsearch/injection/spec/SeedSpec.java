/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.injection.spec;

/**
 * An {@link InjectionSpec} that can represent a requirement that came directly from the user,
 * as opposed to one that was inferred automatically by the injector
 * (though these can <em>also</em> be inferred by the injector).
 */
public sealed interface SeedSpec extends UnambiguousSpec permits ExistingInstanceSpec, MethodHandleSpec {}

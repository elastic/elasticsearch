/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

/**
 * Implementation notes to all components:
 * - a @NamedComponent annotation with a name is required in order for the component to be found by Elasticsearch
 * - a constructor is annotated with @Inject and has a settings interface as an argument. See the javadoc for the
 * ExampleAnalysisSettings for more details
 * - a no/noarg constructor is also possible
 * - a methods from stable analysis api are to be implemented with apache lucene
 */
package org.elasticsearch.example.analysis;

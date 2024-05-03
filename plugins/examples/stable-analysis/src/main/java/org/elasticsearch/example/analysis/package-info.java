/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

/**
 * Implementation notes to all components:
 * <ul>
 * <li>a @NamedComponent annotation with a name is required in order for the component to be found by Elasticsearch</li>
 * <li>a constructor is annotated with @Inject and has a settings interface as an argument. See the javadoc for the</li>
 * </ul>
 * ExampleAnalysisSettings for more details:
 * <ul>
 * <li>a no/noarg constructor is also possible</li>
 * <li>a methods from stable analysis api are to be implemented with Apache Lucene</li>
 * </ul>
 */
package org.elasticsearch.example.analysis;

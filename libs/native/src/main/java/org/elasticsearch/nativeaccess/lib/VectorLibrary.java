/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nativeaccess.lib;

import org.elasticsearch.nativeaccess.VectorScorerFactory;

/**
 * A VectorLibrary is just an adaptation of the factory for a NativeLibrary.
 * It is needed so the NativeLibraryProvider can be the single point of construction
 * for native implementations.
 */
public non-sealed interface VectorLibrary extends NativeLibrary, VectorScorerFactory {}

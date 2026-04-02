/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.plugin;

import org.elasticsearch.plugins.Plugin;

/**
 * Plugin that provides common codec implementations. Codec registration is done
 * via the Lucene SPI mechanism (META-INF/services). This is just required to define codecs common as a module.
 */
public class CodecsCommonPlugin extends Plugin {}

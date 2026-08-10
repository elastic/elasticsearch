/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.foreign;

import java.lang.foreign.MemorySegment;

/**
 * The result of resolving a native symbol: the name that was actually chosen
 * (which may differ from the requested name) and its address.
 *
 * <p>{@code name} is the symbol name that was actually looked up — usually the same as
 * the requested name, but may differ when a {@link SymbolResolver} performs fallback logic.
 * {@code address} is the function pointer for that symbol.
 */
public record ResolvedSymbol(String name, MemorySegment address) {}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec;

/**
 * Interface for providing file pointers to specific sections of term data
 * in a block-based index structure. Implementations of this interface
 * offer access to file pointers for document IDs, positions, and payloads
 * associated with terms. This is used by index disk analyze usage api.
 */
public interface BlockTermStateFPProvider {

    /** file pointer to the start of the doc ids enumeration, in .doc file */
    long getDocStartFP();

    /** file pointer to the start of the positions enumeration, in .pos file */
    long getPosStartFP();

    /** file pointer to the start of the payloads enumeration, in .pay file */
    long getPayStartFP();

}

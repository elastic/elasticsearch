/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

/**
 * Parses a document from a {@link SourceToParse} using the index's current {@link MappingLookup}.
 *
 * @see DefaultDocumentParser
 * @see FlatDocumentParser
 */
public interface DocumentParser {

    /**
     * Parse a document
     *
     * @param source        the document to parse
     * @param mappingLookup the mappings information needed to parse the document
     * @return the parsed document
     * @throws DocumentParsingException whenever there's a problem parsing the document
     */
    ParsedDocument parseDocument(SourceToParse source, MappingLookup mappingLookup) throws DocumentParsingException;

}

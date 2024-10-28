/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.bytes.BytesReference;

public interface DocumentParsingEvent {
    record Value(Object value, BytesReference source) implements DocumentParsingEvent {

    }

    record StringValue(String str) {

    }

    record Object(BytesReference source) implements DocumentParsingEvent {

    }

    record FieldParsed(String name, FieldMapper mapper, DocumentParserContext context) {}

    record ObjectStart(String name) {}

    record ObjectEnd(String name) {}


}

// Mappers - want to just keep parsing stuff as it is, meaning raw xcontentparser stuff

// Dynamic updates ???

// copy_to wants to re-parse stuff when parsing another field

// synthetic source wants to track all offsets, build a document structure, build paths to everything that goes to ignored source and then walk the document

// Just do offset tracking and events specific to synthetic source and see what happens??

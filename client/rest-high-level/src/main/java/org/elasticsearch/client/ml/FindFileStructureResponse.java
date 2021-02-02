/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml;

import org.elasticsearch.client.ml.filestructurefinder.FileStructure;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

public class FindFileStructureResponse implements ToXContentObject {

    private final FileStructure fileStructure;

    FindFileStructureResponse(FileStructure fileStructure) {
        this.fileStructure = Objects.requireNonNull(fileStructure);
    }

    public static FindFileStructureResponse fromXContent(XContentParser parser) throws IOException {
        return new FindFileStructureResponse(FileStructure.PARSER.parse(parser, null).build());
    }

    public FileStructure getFileStructure() {
        return fileStructure;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        fileStructure.toXContent(builder, params);
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(fileStructure);
    }

    @Override
    public boolean equals(Object other) {

        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        FindFileStructureResponse that = (FindFileStructureResponse) other;
        return Objects.equals(fileStructure, that.fileStructure);
    }
}

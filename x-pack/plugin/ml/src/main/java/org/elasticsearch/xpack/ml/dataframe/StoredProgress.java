/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.dataframe;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ml.utils.PhaseProgress;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StoredProgress implements ToXContentObject {

    private static final ParseField PROGRESS = new ParseField("progress");

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<StoredProgress, Void> PARSER = new ConstructingObjectParser<>(
        PROGRESS.getPreferredName(), true, a -> new StoredProgress((List<PhaseProgress>) a[0]));

    static {
        PARSER.declareObjectArray(ConstructingObjectParser.constructorArg(), PhaseProgress.PARSER, PROGRESS);
    }

    private final List<PhaseProgress> progress;

    public StoredProgress(List<PhaseProgress> progress) {
        this.progress = Objects.requireNonNull(progress);
    }

    public List<PhaseProgress> get() {
        return progress;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(PROGRESS.getPreferredName(), progress);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || o.getClass().equals(getClass()) == false) return false;
        StoredProgress that = (StoredProgress) o;
        return Objects.equals(progress, that.progress);
    }

    @Override
    public int hashCode() {
        return Objects.hash(progress);
    }

    public static String documentId(String id) {
        return "data_frame_analytics-" + id + "-progress";
    }

    @Nullable
    public static String extractJobIdFromDocId(String docId) {
        Pattern pattern = Pattern.compile("^data_frame_analytics-(.*)-progress$");
        Matcher matcher = pattern.matcher(docId);
        return matcher.find() ? matcher.group(1) : null;
    }
}

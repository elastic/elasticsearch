/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.job.config.Detector;

import java.io.IOException;
import java.util.Objects;

public class ValidateDetectorAction extends ActionType<AcknowledgedResponse> {

    public static final ValidateDetectorAction INSTANCE = new ValidateDetectorAction();
    public static final String NAME = "cluster:admin/xpack/ml/job/validate/detector";

    protected ValidateDetectorAction() {
        super(NAME, AcknowledgedResponse::readFrom);
    }

    public static class Request extends ActionRequest implements ToXContentObject {

        private Detector detector;

        public static Request parseRequest(XContentParser parser) {
            Detector detector = Detector.STRICT_PARSER.apply(parser, null).build();
            return new Request(detector);
        }

        public Request() {
            this.detector = null;
        }

        public Request(Detector detector) {
            this.detector = detector;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            detector = new Detector(in);
        }

        public Detector getDetector() {
            return detector;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            detector.writeTo(out);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            detector.toXContent(builder, params);
            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hash(detector);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            Request other = (Request) obj;
            return Objects.equals(detector, other.detector);
        }

    }
}

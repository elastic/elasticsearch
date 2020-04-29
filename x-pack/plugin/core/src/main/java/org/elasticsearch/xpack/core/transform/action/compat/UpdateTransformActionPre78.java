/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.transform.action.compat;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfigUpdate;

import java.io.IOException;
import java.util.Objects;

/**
 * In 7.8 update transform has been changed from ordinary request/response objects to tasks request/response.
 * These classes are helpers to translate the old serialization format.
 */
public class UpdateTransformActionPre78 {

    public static class Request extends AcknowledgedRequest<Request> {

        private final TransformConfigUpdate update;
        private final String id;
        private final boolean deferValidation;

        public Request(TransformConfigUpdate update, String id, boolean deferValidation) {
            this.update = update;
            this.id = id;
            this.deferValidation = deferValidation;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            assert in.getVersion().before(Version.V_7_8_0);
            this.update = new TransformConfigUpdate(in);
            this.id = in.readString();
            this.deferValidation = in.readBoolean();
        }

        public static Request fromXContent(final XContentParser parser, final String id, final boolean deferValidation) {
            return new Request(TransformConfigUpdate.fromXContent(parser), id, deferValidation);
        }

        public String getId() {
            return id;
        }

        public boolean isDeferValidation() {
            return deferValidation;
        }

        public TransformConfigUpdate getUpdate() {
            return update;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            assert out.getVersion().before(Version.V_7_8_0);
            super.writeTo(out);
            this.update.writeTo(out);
            out.writeString(id);
            out.writeBoolean(deferValidation);
        }

        @Override
        public int hashCode() {
            return Objects.hash(update, id, deferValidation);
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
            return Objects.equals(update, other.update) && this.deferValidation == other.deferValidation && this.id.equals(other.id);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        private final TransformConfig config;

        public Response(TransformConfig config) {
            this.config = config;
        }

        public Response(StreamInput in) throws IOException {
            assert in.getVersion().before(Version.V_7_8_0);
            this.config = new TransformConfig(in);
        }

        public TransformConfig getConfig() {
            return config;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            assert out.getVersion().before(Version.V_7_8_0);
            this.config.writeTo(out);
        }

        @Override
        public int hashCode() {
            return config.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            Response other = (Response) obj;
            return Objects.equals(config, other.config);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return config.toXContent(builder, params);
        }
    }

}

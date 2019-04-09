package org.elasticsearch.xpack.core.enrich;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.MasterNodeReadRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public class GetEnrichPolicyAction extends Action<GetEnrichPolicyAction.Response> {
    public static final GetEnrichPolicyAction INSTANCE = new GetEnrichPolicyAction();
    public static final String NAME = "indices:admin/xpack/enrich/get";

    private GetEnrichPolicyAction() {
        super(NAME);
    }

    @Override
    public Response newResponse() {
        throw new UnsupportedOperationException("usage of Streamable is to be replaced by Writeable");
    }

    @Override
    public Writeable.Reader<Response> getResponseReader() {
        return Response::new;
    }

    public static class Request extends MasterNodeReadRequest<Request> {

        private String name;

        public Request(String name) {
            this.name = name;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.name = in.readOptionalString();
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeOptionalString(name);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Objects.equals(name, request.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name);
        }
    }

    public static class Response extends ActionResponse implements ToXContentObject {

        public Response() {

        }

        public Response(StreamInput in) throws IOException {
            super.readFrom(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            // placeholder until more data is returned
            return new AcknowledgedResponse(true).toXContent(builder, params);
        }
    }
}

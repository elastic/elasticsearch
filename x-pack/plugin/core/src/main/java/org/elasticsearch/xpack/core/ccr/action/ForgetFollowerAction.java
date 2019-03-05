package org.elasticsearch.xpack.core.ccr.action;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.broadcast.BroadcastRequest;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

public class ForgetFollowerAction extends Action<BroadcastResponse> {

    public static final String ACTION_NAME = "indices:admin/xpack/ccr/forget_follower";
    public static final ForgetFollowerAction INSTANCE = new ForgetFollowerAction();

    private ForgetFollowerAction() {
        super(ACTION_NAME);
    }

    @Override
    public BroadcastResponse newResponse() {
        return new BroadcastResponse();
    }

    public static class Request extends BroadcastRequest<Request> {

        private static final ParseField FOLLOWER_CLUSTER = new ParseField("follower_cluster");
        private static final ParseField FOLLOWER_INDEX = new ParseField("follower_index");
        private static final ParseField FOLLOWER_INDEX_UUID = new ParseField("follower_index_uuid");
        private static final ParseField LEADER_REMOTE_CLUSTER = new ParseField("leader_remote_cluster");

        private static final ObjectParser<String[], Void> PARSER = new ObjectParser<>(ACTION_NAME, () -> new String[4]);

        static {
            PARSER.declareString((parameters, value) -> parameters[0] = value, FOLLOWER_CLUSTER);
            PARSER.declareString((parameters, value) -> parameters[1] = value, FOLLOWER_INDEX);
            PARSER.declareString((parameters, value) -> parameters[2] = value, FOLLOWER_INDEX_UUID);
            PARSER.declareString((parameters, value) -> parameters[3] = value, LEADER_REMOTE_CLUSTER);
        }

        public static ForgetFollowerAction.Request fromXContent(
                final XContentParser parser,
                final String leaderIndex) throws IOException {
            final String[] parameters = PARSER.parse(parser, null);
            return new Request(parameters[0], parameters[1], parameters[2], parameters[3], leaderIndex);
        }

        private String followerCluster;

        public String followerCluster() {
            return followerCluster;
        }

        private String followerIndex;

        public String followerIndex() {
            return followerIndex;
        }

        private String followerIndexUUID;

        public String followerIndexUUID() {
            return followerIndexUUID;
        }

        private String leaderRemoteCluster;

        public String leaderRemoteCluster() {
            return leaderRemoteCluster;
        }

        private String leaderIndex;

        public String leaderIndex() {
            return leaderIndex;
        }

        public Request() {

        }

        public Request(
                final String followerCluster,
                final String followerIndex,
                final String followerIndexUUID,
                final String leaderRemoteCluster,
                final String leaderIndex) {
            super(new String[]{leaderIndex});
            this.followerCluster = Objects.requireNonNull(followerCluster);
            this.leaderIndex = Objects.requireNonNull(leaderIndex);
            this.leaderRemoteCluster = Objects.requireNonNull(leaderRemoteCluster);
            this.followerIndex = Objects.requireNonNull(followerIndex);
            this.followerIndexUUID = Objects.requireNonNull(followerIndexUUID);
        }

        public Request(final StreamInput in) throws IOException {
            super.readFrom(in);
            followerCluster = in.readString();
            leaderIndex = in.readString();
            leaderRemoteCluster = in.readString();
            followerIndex = in.readString();
            followerIndexUUID = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(followerCluster);
            out.writeString(leaderIndex);
            out.writeString(leaderRemoteCluster);
            out.writeString(followerIndex);
            out.writeString(followerIndexUUID);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

    }

    public static class Response extends BroadcastResponse {

    }

}

package org.elasticsearch.plugin.indexbysearch;

import java.io.IOException;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.script.Script;

/**
 * Request to reindex a set of documents where they are without changing their
 * locations or IDs.
 */
public class ReindexInPlaceRequest extends AbstractBulkByScrollRequest<ReindexInPlaceRequest> {
    /**
     * Version type to use on the index requests. Defaults to INFER.
     */
    private ReindexVersionType versionType = ReindexVersionType.INFER;

    private Script script;

    public ReindexInPlaceRequest() {
    }

    public ReindexInPlaceRequest(SearchRequest search) {
        super(search);
    }

    /**
     * Version type to use on the index requests.
     */
    public ReindexVersionType versionType() {
        return versionType;
    }

    /**
     * Version type to use on the index requests.
     */
    public ReindexInPlaceRequest versionType(ReindexVersionType versionType) {
        this.versionType = versionType;
        return this;
    }

    /**
     * Script to use to update the document on reindex.
     */
    public Script script() {
        return script;
    }

    /**
     * Script to use to update the document on reindex.
     */
    public ReindexInPlaceRequest script(Script script) {
        this.script = script;
        return this;
    }

    @Override
    protected ReindexInPlaceRequest self() {
        return this;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        versionType = ReindexVersionType.PROTOTYPE.readFrom(in);
        if (in.readBoolean()) {
            script = Script.readScript(in);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        versionType.writeTo(out);
        out.writeBoolean(script != null);
        if (script != null) {
            script.writeTo(out);
        }
    }

    @Override
    public String toString() {
        StringBuilder b = new StringBuilder();
        b.append("reindex ");
        searchToString(b);
        if (script != null) {
            b.append(" updated with [").append(script).append(']');
        }
        return b.toString();
    }

    public static enum ReindexVersionType implements Writeable<ReindexVersionType> {
        /**
         * Always use the REINDEX version type.
         */
        REINDEX(0) {
            @Override
            public VersionType versionType(ReindexInPlaceRequest request) {
                return VersionType.REINDEX;
            }
        },
        /**
         * Always use the INTERNAL version type.
         */
        INTERNAL(1) {
            @Override
            public VersionType versionType(ReindexInPlaceRequest request) {
                return VersionType.INTERNAL;
            }
        },
        /**
         * Infer the version type from the request. If there is a script then
         * use INTERNAL, if there isn't then use REINDEX.
         */
        INFER(2) {
            @Override
            public VersionType versionType(ReindexInPlaceRequest request) {
                if (request.script() == null) {
                    return VersionType.REINDEX;
                }
                return VersionType.INTERNAL;
            }
        };

        /**
         * Prototype on which to call readFrom to read any instance.
         */
        public static ReindexVersionType PROTOTYPE = REINDEX;

        private final byte id;

        private ReindexVersionType(int id) {
            this.id = (byte) id;
        }

        public abstract VersionType versionType(ReindexInPlaceRequest request);

        public byte id() {
            return id;
        }

        @Override
        public ReindexVersionType readFrom(StreamInput in) throws IOException {
            return fromId(in.readByte());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeByte(id);
        }

        public static ReindexVersionType fromString(String versionType) {
            switch (versionType) {
            case "reindex":
                return REINDEX;
            case "internal":
                return INTERNAL;
            case "infer":
                return INFER;
            default:
                throw new IllegalArgumentException("ReindexVersionType may only be \"internal\", \"reindex\", or \"infer\" but was [" + versionType + "]");
            }
        }

        public static ReindexVersionType fromId(byte id) {
            switch (id) {
            case 0:
                return REINDEX;
            case 1:
                return INTERNAL;
            case 2:
                return INFER;
            default:
                throw new IllegalArgumentException("Unknown id [" + id + "]");
            }
        }
    }
}

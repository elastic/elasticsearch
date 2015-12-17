package org.elasticsearch.plugin.reindex;

import java.io.IOException;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.script.Script;

public abstract class AbstractBulkIndexByScrollRequest<Self extends AbstractBulkIndexByScrollRequest<Self>>
        extends AbstractBulkByScrollRequest<Self> {
    /**
     * Script to modify the documents before they are processed.
     */
    private Script script;

    public AbstractBulkIndexByScrollRequest() {
    }

    public AbstractBulkIndexByScrollRequest(SearchRequest source) {
        super(source);
    }

    /**
     * Script to modify the documents before they are processed.
     */
    public Script script() {
        return script;
    }

    /**
     * Script to modify the documents before they are processed.
     */
    public Self script(@Nullable Script script) {
        this.script = script;
        return self();
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        if (in.readBoolean()) {
            script = Script.readScript(in);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(script != null);
        if (script != null) {
            script.writeTo(out);
        }
    }

    @Override
    protected void searchToString(StringBuilder b) {
        super.searchToString(b);
        if (script != null) {
            b.append(" updated with [").append(script).append(']');
        }
    }
}

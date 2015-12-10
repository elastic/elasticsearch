package org.elasticsearch.plugin.indexbysearch;

import static java.lang.Math.min;
import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.search.sort.SortBuilders.fieldSort;

import java.io.IOException;
import java.util.Arrays;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.builder.SearchSourceBuilder;

public abstract class AbstractBulkByScrollRequest<Self extends AbstractBulkByScrollRequest<Self>>
        extends ActionRequest<Self> {

    private static final TimeValue DEFAULT_SCROLL_TIMEOUT = TimeValue.timeValueMinutes(5);
    private static final int DEFAULT_SIZE = 100;

    /**
     * The search to be executed.
     */
    private SearchRequest search;

    /**
     * Maximum number of processed documents. Defaults to -1 meaning process all
     * documents.
     */
    private int size = -1;

    /**
     * Should version conflicts cause aborts? Defaults to false.
     */
    private boolean abortOnVersionConflict = false;

    public AbstractBulkByScrollRequest() {
    }

    public AbstractBulkByScrollRequest(SearchRequest search) {
        this.search = search;

        // Set the defaults which differ from SearchRequest's defaults.
        search.scroll(DEFAULT_SCROLL_TIMEOUT);
        search.source(new SearchSourceBuilder());
        search.source().version(true);
        search.source().sort(fieldSort("_doc"));
        search.source().size(DEFAULT_SIZE);
    }

    /**
     * `this` cast to Self. Used for building fluent methods without cast
     * warnings.
     */
    protected abstract Self self();

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException e = search.validate();
        if (search.source().from() != -1) {
            e = addValidationError("from is not supported in this context", e);
        }
        return e;
    }

    /**
     * Maximum number of processed documents. Defaults to -1 meaning process all
     * documents.
     */
    public int size() {
        return size;
    }

    /**
     * Maximum number of processed documents. Defaults to -1 meaning process all
     * documents.
     */
    public Self size(int size) {
        this.size = size;
        return self();
    }

    /**
     * Should version conflicts cause aborts? Defaults to false.
     */
    public boolean abortOnVersionConflict() {
        return abortOnVersionConflict;
    }

    /**
     * Should version conflicts cause aborts? Defaults to false.
     */
    public Self abortOnVersionConflict(boolean abortOnVersionConflict) {
        this.abortOnVersionConflict = abortOnVersionConflict;
        return self();
    }

    public SearchRequest search() {
        return search;
    }

    public void fillInConditionalDefaults() {
        if (size() != -1) {
            /*
             * Don't use larger batches than the maximum request size because
             * that'd be silly.
             */
            search().source().size(min(size(), search().source().size()));
        }
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        search.readFrom(in);
        size = in.readVInt();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        search.writeTo(out);
        out.writeVInt(size);
    }

    /**
     * Append a short description of the search request to a StringBuilder. Used
     * to make toString.
     */
    protected void searchToString(StringBuilder b) {
        if (search.indices() != null && search.indices().length != 0) {
            b.append(Arrays.toString(search.indices()));
        } else {
            b.append("[all indices]");
        }
        if (search.types() != null && search.types().length != 0) {
            b.append(search.types());
        }
    }

}

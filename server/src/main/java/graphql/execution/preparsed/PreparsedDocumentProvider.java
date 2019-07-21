package graphql.execution.preparsed;


import graphql.ExecutionInput;
import graphql.PublicSpi;

import java.util.function.Function;

/**
 * Interface that allows clients to hook in Document caching and/or the whitelisting of queries
 */
@PublicSpi
public interface PreparsedDocumentProvider {
    /**
     * This is called to get a "cached" pre-parsed query and if its not present, then the computeFunction
     * can be called to parse and validate the query
     *
     * @param executionInput  The {@link graphql.ExecutionInput} containing the query
     * @param computeFunction If the query has not be pre-parsed, this function can be called to parse it
     *
     * @return an instance of {@link PreparsedDocumentEntry}
     */
    PreparsedDocumentEntry getDocument(ExecutionInput executionInput, Function<ExecutionInput, PreparsedDocumentEntry> computeFunction);
}



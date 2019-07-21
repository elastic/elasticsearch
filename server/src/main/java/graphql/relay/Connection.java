package graphql.relay;

import graphql.PublicApi;

import java.util.List;

/**
 * This represents a connection in Relay, which is a list of {@link graphql.relay.Edge edge}s
 * as well as a {@link graphql.relay.PageInfo pageInfo} that describes the pagination of that list.
 *
 * See <a href="https://facebook.github.io/relay/graphql/connections.htm">https://facebook.github.io/relay/graphql/connections.htm</a>
 */
@PublicApi
public interface Connection<T> {

    /**
     * @return a list of {@link graphql.relay.Edge}s that are really a node of data and its cursor
     */
    List<Edge<T>> getEdges();

    /**
     * @return {@link graphql.relay.PageInfo} pagination data about about that list of edges
     */
    PageInfo getPageInfo();

}

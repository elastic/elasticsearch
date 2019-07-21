package graphql.relay;

import graphql.PublicApi;

/**
 * Represents an edge in Relay which is essentially a node of data T and the cursor for that node.
 *
 * See <a href="https://facebook.github.io/relay/graphql/connections.htm#sec-Edge-Types">https://facebook.github.io/relay/graphql/connections.htm#sec-Edge-Types</a>
 */
@PublicApi
public interface Edge<T> {

    /**
     * @return the node of data that this edge represents
     */
    T getNode();

    /**
     * @return the cursor for this edge node
     */
    ConnectionCursor getCursor();

}

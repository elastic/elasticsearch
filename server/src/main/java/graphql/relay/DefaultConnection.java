package graphql.relay;

import graphql.PublicApi;

import java.util.Collections;
import java.util.List;

import static graphql.Assert.assertNotNull;
import static java.util.Collections.unmodifiableList;

/**
 * A default implementation of {@link graphql.relay.Connection}
 */
@PublicApi
public class DefaultConnection<T> implements Connection<T> {

    private final List<Edge<T>> edges;
    private final PageInfo pageInfo;

    /**
     * A connection consists of a list of edges and page info
     *
     * @param edges    a non null list of edges
     * @param pageInfo a non null page info
     *
     * @throws IllegalArgumentException if edges or page info is null. use {@link Collections#emptyList()} for empty edges.
     */
    public DefaultConnection(List<Edge<T>> edges, PageInfo pageInfo) {
        this.edges = unmodifiableList(assertNotNull(edges, "edges cannot be null"));
        this.pageInfo = assertNotNull(pageInfo, "page info cannot be null");
    }

    @Override
    public List<Edge<T>> getEdges() {
        return edges;
    }

    @Override
    public PageInfo getPageInfo() {
        return pageInfo;
    }

    @Override
    public String toString() {
        return "DefaultConnection{" +
                "edges=" + edges +
                ", pageInfo=" + pageInfo +
                '}';
    }
}

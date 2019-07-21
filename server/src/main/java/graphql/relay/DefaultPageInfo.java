package graphql.relay;


import graphql.PublicApi;

@PublicApi
public class DefaultPageInfo implements PageInfo {

    private final ConnectionCursor startCursor;
    private final ConnectionCursor endCursor;
    private final boolean hasPreviousPage;
    private final boolean hasNextPage;

    public DefaultPageInfo(ConnectionCursor startCursor, ConnectionCursor endCursor, boolean hasPreviousPage, boolean hasNextPage) {
        this.startCursor = startCursor;
        this.endCursor = endCursor;
        this.hasPreviousPage = hasPreviousPage;
        this.hasNextPage = hasNextPage;
    }

    @Override
    public ConnectionCursor getStartCursor() {
        return startCursor;
    }


    @Override
    public ConnectionCursor getEndCursor() {
        return endCursor;
    }

    @Override
    public boolean isHasPreviousPage() {
        return hasPreviousPage;
    }

    @Override
    public boolean isHasNextPage() {
        return hasNextPage;
    }

    @Override
    public String toString() {
        return "DefaultPageInfo{" +
                " startCursor=" + startCursor +
                ", endCursor=" + endCursor +
                ", hasPreviousPage=" + hasPreviousPage +
                ", hasNextPage=" + hasNextPage +
                '}';
    }
}

package graphql.relay;

import graphql.PublicApi;
import graphql.TrivialDataFetcher;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static graphql.Assert.assertNotNull;
import static graphql.Assert.assertTrue;
import static java.lang.String.format;
import static java.util.Base64.getDecoder;
import static java.util.Base64.getEncoder;

@PublicApi
public class SimpleListConnection<T> implements DataFetcher<Connection<T>>, TrivialDataFetcher<Connection<T>> {

    static final String DUMMY_CURSOR_PREFIX = "simple-cursor";
    private final String prefix;
    private final List<T> data;

    public SimpleListConnection(List<T> data, String prefix) {
        this.data = assertNotNull(data, " data cannot be null");
        assertTrue(prefix != null && !prefix.isEmpty(), "prefix cannot be null or empty");
        this.prefix = prefix;
    }

    public SimpleListConnection(List<T> data) {
        this(data, DUMMY_CURSOR_PREFIX);
    }

    private List<Edge<T>> buildEdges() {
        List<Edge<T>> edges = new ArrayList<>();
        int ix = 0;
        for (T object : data) {
            edges.add(new DefaultEdge<>(object, new DefaultConnectionCursor(createCursor(ix++))));
        }
        return edges;
    }

    @Override
    public Connection<T> get(DataFetchingEnvironment environment) {

        List<Edge<T>> edges = buildEdges();

        if (edges.size() == 0) {
            return emptyConnection();
        }

        ConnectionCursor firstPresliceCursor = edges.get(0).getCursor();
        ConnectionCursor lastPresliceCursor = edges.get(edges.size() - 1).getCursor();

        int afterOffset = getOffsetFromCursor(environment.getArgument("after"), -1);
        int begin = Math.max(afterOffset, -1) + 1;
        int beforeOffset = getOffsetFromCursor(environment.getArgument("before"), edges.size());
        int end = Math.min(beforeOffset, edges.size());

        if (begin > end) begin = end;

        edges = edges.subList(begin, end);
        if (edges.size() == 0) {
            return emptyConnection();
        }

        Integer first = environment.getArgument("first");
        Integer last = environment.getArgument("last");

        if (first != null) {
            if (first < 0) {
                throw new InvalidPageSizeException(format("The page size must not be negative: 'first'=%s", first));
            }
            edges = edges.subList(0, first <= edges.size() ? first : edges.size());
        }
        if (last != null) {
            if (last < 0) {
                throw new InvalidPageSizeException(format("The page size must not be negative: 'last'=%s", last));
            }
            edges = edges.subList(last > edges.size() ? 0 : edges.size() - last, edges.size());
        }

        if (edges.isEmpty()) {
            return emptyConnection();
        }

        Edge<T> firstEdge = edges.get(0);
        Edge<T> lastEdge = edges.get(edges.size() - 1);

        PageInfo pageInfo = new DefaultPageInfo(
                firstEdge.getCursor(),
                lastEdge.getCursor(),
                !firstEdge.getCursor().equals(firstPresliceCursor),
                !lastEdge.getCursor().equals(lastPresliceCursor)
        );

        return new DefaultConnection<>(
                edges,
                pageInfo
        );
    }

    private Connection<T> emptyConnection() {
        PageInfo pageInfo = new DefaultPageInfo(null, null, false, false);
        return new DefaultConnection<>(Collections.emptyList(), pageInfo);
    }

    /**
     * find the object's cursor, or null if the object is not in this connection.
     *
     * @param object the object in play
     *
     * @return a connection cursor
     */
    public ConnectionCursor cursorForObjectInConnection(T object) {
        int index = data.indexOf(object);
        if (index == -1) {
            return null;
        }
        String cursor = createCursor(index);
        return new DefaultConnectionCursor(cursor);
    }

    private int getOffsetFromCursor(String cursor, int defaultValue) {
        if (cursor == null) {
            return defaultValue;
        }
        byte[] decode;
        try {
            decode = getDecoder().decode(cursor);
        } catch (IllegalArgumentException e) {
            throw new InvalidCursorException(format("The cursor is not in base64 format : '%s'", cursor), e);
        }
        String string = new String(decode, StandardCharsets.UTF_8);
        if (prefix.length() > string.length()) {
            throw new InvalidCursorException(format("The cursor prefix is missing from the cursor : '%s'", cursor));
        }
        try {
            return Integer.parseInt(string.substring(prefix.length()));
        } catch (NumberFormatException nfe) {
            throw new InvalidCursorException(format("The cursor was not created by this class  : '%s'", cursor), nfe);
        }
    }

    private String createCursor(int offset) {
        byte[] bytes = (prefix + Integer.toString(offset)).getBytes(StandardCharsets.UTF_8);
        return getEncoder().encodeToString(bytes);
    }
}

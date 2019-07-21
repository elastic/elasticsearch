package graphql.relay;

import graphql.PublicApi;

/**
 * Represents a {@link Connection connection} cursor in Relay which is an opaque
 * string that the server understands.  Often this is base64 encoded but the spec only
 * mandates that it be an opaque cursor so meaning can't be inferred from it (to prevent cheating like
 * pre calculating the next cursor on the client say)
 *
 * See <a href="https://facebook.github.io/relay/graphql/connections.htm#sec-Cursor">https://facebook.github.io/relay/graphql/connections.htm#sec-Cursor</a>
 */
@PublicApi
public interface ConnectionCursor {

    /**
     * @return an opaque string that represents this cursor.
     */
    String getValue();

}

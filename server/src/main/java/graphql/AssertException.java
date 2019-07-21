package graphql;


@PublicApi
public class AssertException extends GraphQLException {

    public AssertException(String message) {
        super(message);
    }
}

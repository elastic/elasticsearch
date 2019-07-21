package graphql.schema.idl.errors;

public class SchemaMissingError extends BaseError {

    public SchemaMissingError() {
        super(null, "There is no top level schema object defined");
    }
}

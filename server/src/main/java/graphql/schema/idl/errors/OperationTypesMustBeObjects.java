package graphql.schema.idl.errors;

import graphql.language.OperationTypeDefinition;

import static java.lang.String.format;

public class OperationTypesMustBeObjects extends BaseError {

    public OperationTypesMustBeObjects(OperationTypeDefinition op) {
        super(op, format("The operation type '%s' MUST have a object type as its definition %s",
                op.getName(), lineCol(op)));
    }
}

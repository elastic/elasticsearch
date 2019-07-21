package graphql.schema.idl.errors;

import graphql.language.DirectiveDefinition;

import static java.lang.String.format;

public class DirectiveRedefinitionError extends BaseError {

    public DirectiveRedefinitionError(DirectiveDefinition newEntry, DirectiveDefinition oldEntry) {
        super(oldEntry,
                format("'%s' type %s tried to redefine existing directive '%s' type %s",
                        newEntry.getName(), BaseError.lineCol(newEntry), oldEntry.getName(), BaseError.lineCol(oldEntry)
                ));
    }
}

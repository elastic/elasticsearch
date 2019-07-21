package graphql.schema.idl.errors;

import graphql.language.TypeDefinition;

import static java.lang.String.format;

public class TypeRedefinitionError extends BaseError {

    public TypeRedefinitionError(TypeDefinition newEntry, TypeDefinition oldEntry) {
        super(oldEntry,
                format("'%s' type %s tried to redefine existing '%s' type %s",
                        newEntry.getName(), BaseError.lineCol(newEntry), oldEntry.getName(), BaseError.lineCol(oldEntry)
                ));
    }
}

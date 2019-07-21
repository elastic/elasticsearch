package graphql.schema.idl;

import graphql.PublicSpi;

/**
 * Provides the Java runtime value for each graphql Enum value. Used for IDL driven schema creation.
 * <p>
 * Enum values are considered static: This is called when a schema is created. It is not used when a query is executed.
 */
@PublicSpi
public interface EnumValuesProvider {

    /**
     * @param name an Enum value
     *
     * @return not null
     */
    Object getValue(String name);

}

package graphql.schema.idl;

import graphql.schema.DataFetcher;
import graphql.schema.GraphQLScalarType;
import graphql.schema.PropertyDataFetcher;
import graphql.schema.TypeResolver;

import static graphql.Assert.assertShouldNeverHappen;

public class NoopWiringFactory implements WiringFactory {

    @Override
    public boolean providesScalar(ScalarWiringEnvironment environment) {
        return false;
    }

    @Override
    public GraphQLScalarType getScalar(ScalarWiringEnvironment environment) {
        return assertShouldNeverHappen();
    }

    @Override
    public boolean providesTypeResolver(InterfaceWiringEnvironment environment) {
        return false;
    }

    @Override
    public TypeResolver getTypeResolver(InterfaceWiringEnvironment environment) {
        return assertShouldNeverHappen();
    }

    @Override
    public boolean providesTypeResolver(UnionWiringEnvironment environment) {
        return false;
    }

    @Override
    public TypeResolver getTypeResolver(UnionWiringEnvironment environment) {
        return assertShouldNeverHappen();
    }

    @Override
    public boolean providesDataFetcher(FieldWiringEnvironment environment) {
        return false;
    }

    @Override
    public DataFetcher getDataFetcher(FieldWiringEnvironment environment) {
        return assertShouldNeverHappen();
    }

    @Override
    public DataFetcher getDefaultDataFetcher(FieldWiringEnvironment environment) {
        return null;
    }
}

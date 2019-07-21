package graphql.schema.idl;

import graphql.Assert;
import graphql.PublicApi;

import java.util.Map;

@PublicApi
public class MapEnumValuesProvider implements EnumValuesProvider {


    private final Map<String, Object> values;

    public MapEnumValuesProvider(Map<String, Object> values) {
        Assert.assertNotNull(values, "values can't be null");
        this.values = values;
    }

    @Override
    public Object getValue(String name) {
        return values.get(name);
    }
}

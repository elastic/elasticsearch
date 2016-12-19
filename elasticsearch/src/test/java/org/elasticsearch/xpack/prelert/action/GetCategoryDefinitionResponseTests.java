/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
        CategoryDefinition definition = new CategoryDefinition(randomAsciiOfLength(10));
        QueryPage<CategoryDefinition> queryPage =
                new QueryPage<>(Collections.singletonList(definition), 1L, CategoryDefinition.RESULTS_FIELD);
        return new GetCategoriesDefinitionAction.Response(queryPage);
    }

    @Override
    protected GetCategoriesDefinitionAction.Response createBlankInstance() {
        return new GetCategoriesDefinitionAction.Response();
    }
}

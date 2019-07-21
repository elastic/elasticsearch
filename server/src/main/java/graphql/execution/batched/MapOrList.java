package graphql.execution.batched;

import graphql.Internal;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Internal
@Deprecated
public class MapOrList {


    private Map<String, Object> map;
    private List<Object> list;

    public static MapOrList createList(List<Object> list) {
        MapOrList mapOrList = new MapOrList();
        mapOrList.list = list;
        return mapOrList;
    }

    public static MapOrList createMap(Map<String, Object> map) {
        MapOrList mapOrList = new MapOrList();
        mapOrList.map = map;
        return mapOrList;
    }

    public MapOrList createAndPutMap(String key) {
        Map<String, Object> map = new LinkedHashMap<>();
        putOrAdd(key, map);
        return createMap(map);
    }

    public MapOrList createAndPutList(String key) {
        List<Object> resultList = new ArrayList<>();
        putOrAdd(key, resultList);
        return createList(resultList);
    }

    public void putOrAdd(String fieldName, Object value) {
        if (this.map != null) {
            this.map.put(fieldName, value);
        } else {
            this.list.add(value);
        }
    }


    public Object toObject() {
        if (this.map != null) {
            return this.map;
        } else {
            return this.list;
        }
    }
}

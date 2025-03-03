## `VALUES` [esql-values]

::::{warning}
Do not use on production environments. This functionality is in technical preview and may be changed or removed in a future release. Elastic will work to fix any issues, but features in technical preview are not subject to the support SLA of official GA features.
::::


**Syntax**

:::{image} ../../../../../images/values.svg
:alt: Embedded
:class: text-center
:::

**Parameters**

true
**Description**

Returns all values in a group as a multivalued field. The order of the returned values isn’t guaranteed. If you need the values returned in order use [`MV_SORT`](../../esql-functions-operators.md#esql-mv_sort).

**Supported types**

| field | result |
| --- | --- |
| boolean | boolean |
| date | date |
| date_nanos | date_nanos |
| double | double |
| integer | integer |
| ip | ip |
| keyword | keyword |
| long | long |
| text | keyword |
| version | version |

**Example**

```esql
  FROM employees
| EVAL first_letter = SUBSTRING(first_name, 0, 1)
| STATS first_name=MV_SORT(VALUES(first_name)) BY first_letter
| SORT first_letter
```

| first_name:keyword | first_letter:keyword |
| --- | --- |
| [Alejandro, Amabile, Anneke, Anoosh, Arumugam] | A |
| [Basil, Berhard, Berni, Bezalel, Bojan, Breannda, Brendon] | B |
| [Charlene, Chirstian, Claudi, Cristinel] | C |
| [Danel, Divier, Domenick, Duangkaew] | D |
| [Ebbe, Eberhardt, Erez] | E |
| Florian | F |
| [Gao, Georgi, Georgy, Gino, Guoxiang] | G |
| [Heping, Hidefumi, Hilari, Hironobu, Hironoby, Hisao] | H |
| [Jayson, Jungsoon] | J |
| [Kazuhide, Kazuhito, Kendra, Kenroku, Kshitij, Kwee, Kyoichi] | K |
| [Lillian, Lucien] | L |
| [Magy, Margareta, Mary, Mayuko, Mayumi, Mingsen, Mokhtar, Mona, Moss] | M |
| Otmar | O |
| [Parto, Parviz, Patricio, Prasadram, Premal] | P |
| [Ramzi, Remzi, Reuven] | R |
| [Sailaja, Saniya, Sanjiv, Satosi, Shahaf, Shir, Somnath, Sreekrishna, Sudharsan, Sumant, Suzette] | S |
| [Tse, Tuval, Tzvetan] | T |
| [Udi, Uri] | U |
| [Valdiodio, Valter, Vishv] | V |
| Weiyi | W |
| Xinglin | X |
| [Yinghua, Yishay, Yongqiao] | Y |
| [Zhongwei, Zvonko] | Z |
| null | null |

::::{warning}
This can use a significant amount of memory and ES|QL doesn’t yet grow aggregations beyond memory. So this aggregation will work until it is used to collect more values than can fit into memory. Once it collects too many values it will fail the query with a [Circuit Breaker Error](docs-content://troubleshoot/elasticsearch/circuit-breaker-errors.md).

::::




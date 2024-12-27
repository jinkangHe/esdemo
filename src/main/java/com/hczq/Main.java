package com.hczq;

import cn.hutool.core.util.IdUtil;
import cn.hutool.core.util.ObjectUtil;
import cn.hutool.core.util.RandomUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.*;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregation;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.TermsValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.nested.NestedAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.Avg;
import org.elasticsearch.search.aggregations.metrics.Cardinality;
import org.elasticsearch.search.aggregations.metrics.Sum;
import org.elasticsearch.search.aggregations.metrics.SumAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.time.LocalDate;
import java.util.*;

@Slf4j
public class Main {

    private static RestHighLevelClient client;
    public static void main(String[] args) throws Exception {
        client = createEsClient();
        //createIndex();

        /*indexInfo();
        for (int i = 0; i < 50; i++) {
            addDoc();

        }*/
//        fuzzySearchDoc();
//        System.out.println("=======");
//        prefixSearch();
//        System.out.println("=======");
//        wildSearch();
         //   nestedSearch();
        //rangeSearch();
        //combineSearch();
        //regexSearch();

        groupByAge();
//        groupByBuyTimes();
//        avgAge();
//        cardinalityAggregationByAge();
//        groupByBirthday();
//        groupByNameAndAge(null);

       sumBuyTimesGroupByAge();
    }

    //建立es链接
    public static RestHighLevelClient createEsClient() {
        // 创建客户端
        return new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("192.168.1.126", 9200, "http")
                )
        );
    }


    //创建索引
    public static void createIndex() {
        // 创建索引请求
        CreateIndexRequest request = new CreateIndexRequest("esdemo");
        //设置mapping
        String mapping = "{\n" +
                "  \"properties\": {\n" +
                "    \"name\": {\n" +
                "      \"type\": \"text\"\n" +
                "    },\n" +
                "    \"age\": {\n" +
                "      \"type\": \"integer\"\n" +
                "    },\n" +
                "    \"birthday\": {\n" +
                "      \"type\": \"date\"\n" +
                "    },\n" +
                "    \"subjectInfo\": {\n" +
                "      \"type\": \"nested\",\n" +
                "      \"properties\": {\n" +
                "        \"subject\": {\n" +
                "          \"type\": \"text\"\n" +
                "        },\n" +
                "        \"score\": {\n" +
                "          \"type\": \"integer\"\n" +
                "        }\n" +
                "      }\n" +
                "    },\n" +
                "    \"buyTimes\": {\n" +
                "      \"type\": \"integer\"\n" +
                "    }\n" +
                "  }\n" +
                "}";
        request.mapping(mapping, XContentType.JSON);

        try {
            // 客户端执行请求
            CreateIndexResponse createIndexResponse = client.indices().create(request, RequestOptions.DEFAULT);
            // 得到响应
            boolean acknowledged = createIndexResponse.isAcknowledged();
            System.out.println("索引创建成功：" + acknowledged);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public static void indexInfo() {
        //查看索引信息
        GetIndexRequest request = new GetIndexRequest("esdemo");
        try {
            // 客户端执行请求
            GetIndexResponse getIndexResponse = client.indices().get(request, RequestOptions.DEFAULT);
            //打印索引信息
            System.out.println(getIndexResponse.getAliases());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }




    //新增EsEntity数据
    public static void addDoc() {
        //构建EsEntity数据
        EsEntity esEntity = buildEsEntity();
        IndexRequest request = new IndexRequest("esdemo");
        request.id(StrUtil.toString(IdUtil.randomUUID()));
        request.source(JSONUtil.toJsonStr(esEntity), XContentType.JSON);
        try {
            IndexResponse indexResponse = client.index(request, RequestOptions.DEFAULT);
            System.out.println("新增文档成功：" + indexResponse.getResult());
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private static SearchRequest searchRequest = new SearchRequest("esdemo");
    private static  SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

    public static void fuzzySearchDoc() throws IOException {
       //模糊查询姓张的人
        //不对查询的词汇进行分词 及查询字段中包含张三的
        searchSourceBuilder.query(QueryBuilders.matchPhraseQuery("name", "张三"));
        //对查询的词汇进行分词 查询字段中包含张或者三的
        //searchSourceBuilder.query(QueryBuilders.matchQuery("name", "张三"));
        //精确查询，必须等于张三 而且字段要有keyword类型
        //searchSourceBuilder.query(QueryBuilders.termQuery("name", "张三"));

        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits hits = searchResponse.getHits();
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
        }
    }


    public static void wildSearch() throws IOException {
        //模糊查询姓张的人

        //name为张开头 此处必须用keyword 因为name字段被分词了
        searchSourceBuilder.query(QueryBuilders.wildcardQuery("name", "*9"));
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits hits = searchResponse.getHits();
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
        }
    }

    public static void prefixSearch() throws IOException {
        //模糊查询姓张的人

        //name为张开头 此处必须用keyword 因为name字段被分词了
        searchSourceBuilder.query(QueryBuilders.prefixQuery("name.keyword", "张"));
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits hits = searchResponse.getHits();
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
        }
    }

    public static void rangeSearch() throws IOException {

        searchSourceBuilder.query(QueryBuilders.rangeQuery("birthday").gte("2005-01-01").lte("2014-12-31"));
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits hits = searchResponse.getHits();
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
        }
    }

    //组合查询 buyTimes > 60 并且 名字里面包含张 或者 buyTimes > 30 并且 名字里面包含赵
    public static void combineSearch() throws IOException {
        searchSourceBuilder.query(QueryBuilders
                .boolQuery()
                .must(QueryBuilders.rangeQuery("age").gte(18))
                .must(QueryBuilders.boolQuery()
                        // 第一部分：buyTimes > 60 并且 名字包含 "张"
                        .should(QueryBuilders.boolQuery()
                                .must(QueryBuilders.rangeQuery("buyTimes").gt(60))
                                .must(QueryBuilders.matchQuery("name", "张"))
                        )
                        // 第二部分：buyTimes > 30 并且 名字包含 "赵"
                        .should(QueryBuilders.boolQuery()
                                .must(QueryBuilders.rangeQuery("buyTimes").gt(30))
                                .must(QueryBuilders.matchQuery("name", "赵"))
                        ))



        );
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits hits = searchResponse.getHits();
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
        }
    }


    //正则查询 查询名第一个字为王 第三个字是若的文档
    public static void regexSearch() throws IOException {
        searchSourceBuilder.query(QueryBuilders.regexpQuery("name.keyword", "王.*若"));
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits hits = searchResponse.getHits();
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
        }
    }


    public static void nestedSearch() throws IOException {
        //查询语文成绩大于60分的人
        // 构建查询，查找 subjectInfo.subject 为 "语文" 且 subjectInfo.score 大于 60 的文档
        searchSourceBuilder.query(QueryBuilders.nestedQuery(
                "subjectInfo", // 嵌套字段路径
                QueryBuilders.boolQuery()
                        .must( QueryBuilders.matchQuery("subjectInfo.subject", "语文"))
                        .must(QueryBuilders.rangeQuery("subjectInfo.score").gt(60))
                ,
                ScoreMode.None // 设置评分模式
        ));

        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits hits = searchResponse.getHits();

        // 输出每个匹配的文档
        for (SearchHit hit : hits) {
            System.out.println(hit.getSourceAsString());
        }
    }

    //根据年龄分组

    /**
     * SearchSourceBuilder 是 Elasticsearch 的一个 Java 类，用于构建搜索请求的查询部分。它是 SearchRequest 中的一个重要组成部分，允许你设置查询、聚合、排序、过滤器等条件，以便构建一个完整的查询请求。
     *
     * 简而言之，SearchSourceBuilder 是用来构造和配置 Elasticsearch 搜索请求（SearchRequest）的查询条件、聚合操作、排序规则等内容的工具类
     * @throws IOException
     */
    public static void groupByAge() throws IOException {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.aggregation(
                AggregationBuilders.terms("ageCount")
                        .field("age")

        );
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        Aggregations aggregations = searchResponse.getAggregations();
        Terms categories = aggregations.get("ageCount");
        for (Terms.Bucket bucket : categories.getBuckets()) {
            System.out.println("age12: " + bucket.getKeyAsString() + ", Count: " + bucket.getDocCount());
        }
    }


    public static void groupByBuyTimes() throws IOException {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.aggregation(
                AggregationBuilders.histogram("buyTimesHistogram")
                        .field("buyTimes")
                        .interval(10)  // 每个区间 10 次购买
        );
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        Aggregations aggregations = searchResponse.getAggregations();
        Histogram buyTimesHistogram = aggregations.get("buyTimesHistogram");
        for (Histogram.Bucket bucket : buyTimesHistogram.getBuckets()) {
            System.out.println("Range: " + bucket.getKeyAsString() + ", Count: " + bucket.getDocCount());
        }
    }

    public static void avgAge() throws IOException {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.aggregation(
                AggregationBuilders.avg("averageAge")
                        .field("age")
        );
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        Aggregations aggregations = searchResponse.getAggregations();
        Avg averageAge = aggregations.get("averageAge");
        System.out.println("Average Age: " + averageAge.getValue());
    }

    public static void cardinalityAggregationByAge() throws IOException {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        // 使用 cardinality 聚合来计算不同年龄的数量
        searchSourceBuilder.aggregation(
                AggregationBuilders.cardinality("unique_ages")
                        .field("age")  // 计算 age 字段中不同值的数量
        );

        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

        Aggregations aggregations = searchResponse.getAggregations();
        Cardinality cardinality = aggregations.get("unique_ages");

        // 打印不同年龄的数量
        System.out.println("Unique Ages Count: " + cardinality.getValue());
    }

    //出生日期直方图
    public static void groupByBirthday() throws IOException {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.aggregation(
                AggregationBuilders.dateHistogram("birthdayHistogram")
                        .field("birthday")
                        .calendarInterval(DateHistogramInterval.YEAR)
        );
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        Aggregations aggregations = searchResponse.getAggregations();
        Histogram birthdayHistogram = aggregations.get("birthdayHistogram");
        for (Histogram.Bucket bucket : birthdayHistogram.getBuckets()) {
            System.out.println("Year: " + bucket.getKeyAsString() + ", Count: " + bucket.getDocCount());
        }
    }

    public static void groupByNameAndAge(Map<String,Object> afterKey) throws IOException {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        List<CompositeValuesSourceBuilder<?>> sources = new ArrayList<>();
        sources.add(new TermsValuesSourceBuilder("name").field("name.keyword"));
        sources.add(new TermsValuesSourceBuilder("age").field("age"));

        CompositeAggregationBuilder composite =new CompositeAggregationBuilder("name_age", sources);
        composite.aggregateAfter(afterKey);
        // 创建复合聚合（Composite Aggregation），根据 name 和 age 字段进行分组
        searchSourceBuilder.aggregation(composite);

        // 创建搜索请求并执行查询
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

        // 获取聚合结果
        Aggregations aggregations = searchResponse.getAggregations();
        CompositeAggregation compositeAggregation = aggregations.get("name_age");
        List<? extends CompositeAggregation.Bucket> buckets = compositeAggregation.getBuckets();
        if (ObjectUtil.isEmpty(buckets)) {
            return;
        }
        // 输出每个分组的结果
        for (CompositeAggregation.Bucket bucket : buckets) {
            String name = bucket.getKeyAsString();

            System.out.println("key: " + name +  ", Doc Count: " + bucket.getDocCount());
        }
        // 检查是否还有更多桶
        Map<String, Object> nextAfterKey = compositeAggregation.getBuckets().get(compositeAggregation.getBuckets().size() - 1).getKey();
        if (nextAfterKey != null) {
            // 继续分页获取更多桶
            groupByNameAndAge(nextAfterKey);
        }
    }

    public static void sumBuyTimesGroupByAge() throws IOException {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        // 1. 按照 "age" 字段进行分组
        TermsAggregationBuilder ageAggregation = AggregationBuilders.terms("age_terms")
                .field("age") // 按照 "age" 字段进行分组
                .size(1000)
                .order(BucketOrder.aggregation("total_buy_times", true))
                ;  // 设置每次返回最多 1000 个分组
        // 2. 使用 Sum 聚合来计算 "buyTimes" 字段的总和
        SumAggregationBuilder totalBuyTimesAggregation = AggregationBuilders.sum("total_buy_times")
                .field("buyTimes");  // 计算 "buyTimes" 字段的总和
        ageAggregation.subAggregation(totalBuyTimesAggregation);


        // 创建搜索请求并执行查询
        searchRequest.source(searchSourceBuilder.aggregation(ageAggregation));
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

        // 获取聚合结果
        Aggregations aggregations = searchResponse.getAggregations();
        Terms ageTerms = aggregations.get("age_terms");

        // 输出每个分组的结果
        for (Terms.Bucket bucket : ageTerms.getBuckets()) {  // 使用 Terms.Bucket 类型
            Long age = (Long) bucket.getKey();  // 获取 "age"

            // 获取嵌套聚合的结果
            Sum aggregation = bucket.getAggregations().get("total_buy_times");
            System.out.println("age: " + age + ", totalBuyTimes: " + aggregation.getValue());
        }
    }


    //构建随机EsEntity数据
    public static EsEntity buildEsEntity() {
        EsEntity esEntity = new EsEntity();
        //随机生成姓名
        esEntity.setName(generateRandomName());
        esEntity.setAge(RandomUtil.randomInt(1, 80));
        //随机时间
        esEntity.setBirthday(LocalDate.of(RandomUtil.randomInt(1970, 2020), RandomUtil.randomInt(1, 12), RandomUtil.randomInt(1, 28)));
        ArrayList<SubjectInfo> subjectInfos = new ArrayList<>();
        SubjectInfo yuwen = new SubjectInfo();
        yuwen.setSubject("语文");
        yuwen.setScore(RandomUtil.randomInt(30, 100));
        subjectInfos.add(yuwen);
        SubjectInfo shuxue = new SubjectInfo();
        shuxue.setSubject("数学");
        shuxue.setScore(RandomUtil.randomInt(30, 100));
        subjectInfos.add(shuxue);
        esEntity.setSubjectInfo(subjectInfos);
        esEntity.setBuyTimes(RandomUtil.randomInt(100));
        return esEntity;
    }


    public static String generateRandomName() {
        Random random = new Random();

        // 随机选择姓氏和两个字的名字
        String lastName = lastNames[random.nextInt(lastNames.length)];
        String firstName = firstNames[random.nextInt(firstNames.length)];

        // 生成一个随机的三字中文名字
        return lastName + firstName;
    }

    private static final String[] lastNames = {
            "赵", "钱", "孙", "李", "周", "吴", "郑", "王", "冯", "陈",
            "褚", "卫", "蒋", "沈", "韩", "杨", "朱", "秦", "尤", "许",
            "何", "吕", "施", "张", "孔", "曹", "严", "华", "金", "魏",
            "陶", "姜", "戚", "谢", "邹", "喻", "柏", "水", "窦", "章"
    };

    // 常见的名字列表，增加更多两个字的名字样本
    private static final String[] firstNames = {
            "伟", "芳", "娜", "敏", "静", "强", "磊", "洋", "婷", "秀英",
            "勇", "梅", "杰", "丽", "娟", "莉", "萍", "峰", "军", "鹏",
            "超", "俊", "琳", "浩", "婷", "东", "波", "雪", "红", "琳",
            "旭东", "子晴", "嘉欣", "文婷", "志强", "宇晨", "子涵", "思彤", "诗雨", "梓萱",
            "雨婷", "雅婷", "家豪", "文博", "子轩", "晨曦", "婉儿", "涵琳", "嘉瑞", "梓睿",
            "思源", "一鸣", "晨阳", "瑾萱", "子彤", "佩琳", "奕婷", "美琳", "子琪", "雪梅",
            "紫萱", "思涵", "月婷", "静怡", "志远", "欣怡", "佳琪", "馨月", "诗涵", "睿思",
            "子琪", "婷婷", "俊杰", "宇航", "子睿", "芷若", "怡君", "子瑶", "雅静", "舒婷"
    };
}
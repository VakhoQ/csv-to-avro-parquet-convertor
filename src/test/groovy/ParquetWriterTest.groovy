import com.bigdata.avro.schema.Destination
import com.bigdata.avro.schema.SampleSubmission
import com.bigdata.avro.schema.Test
import com.bigdata.avro.schema.Train
import com.bigdata.utils.ParquetDataWorker
import org.apache.avro.Schema
import org.apache.commons.io.FileUtils
import spock.lang.Specification

import java.nio.file.Paths

class ParquetWriterTest extends Specification {
    def output

    def cleanup() {
        FileUtils.deleteQuietly(output as File)
    }

    def "create parquet Destination record"() {

        given:
        ParquetDataWorker<Destination> reader = new ParquetDataWorker<>()
        String baseDir = getBaseDir()
        String input = baseDir + File.separator + "destinations.csv"
        output = File.createTempFile("csv-", ".parquet")
        Schema schema = Destination.getClassSchema()

        when:
        ParquetDataWorker.csvToParquet(input, output.getAbsolutePath(), schema)

        then:
        noExceptionThrown()
        reader.parquetToObj(output).size() == 9
        reader.parquetToObj(output).get(0).getSrchDestinationId() == 0
        reader.parquetToObj(output).get(1).getSrchDestinationId() == 1
        reader.parquetToObj(output).get(0).getD1().toString() == "-2.19865708695"
        reader.parquetToObj(output).get(1).getD1().toString() == "-2.18169033283"
        reader.parquetToObj(output).get(2).getD1().toString() == "-2.18348974514"
        reader.parquetToObj(output).get(3).getD1().toString() == "-2.17740922654"

    }


    def "create parquet Test record"() {

        given:
        ParquetDataWorker<Test> reader = new ParquetDataWorker<>()

        String baseDir = getBaseDir()
        String input = baseDir + File.separator + "test.csv"
        output = File.createTempFile("csv-", ".parquet")
        Schema schema = Test.getClassSchema()

        when:
        ParquetDataWorker.csvToParquet(input, output.getAbsolutePath(), schema)

        then:
        noExceptionThrown()
        reader.parquetToObj(output).size() == 3
        reader.parquetToObj(output).get(index).get("date_time").toString() == date_time as String
        reader.parquetToObj(output).get(index).get("site_name").toString() == site_name as String
        reader.parquetToObj(output).get(index).get("posa_continent").toString() == posa_continent as String
        reader.parquetToObj(output).get(index).get("user_location_country").toString() == user_location_country as String
        reader.parquetToObj(output).get(index).get("user_location_region").toString() == user_location_region as String
        reader.parquetToObj(output).get(index).get("user_location_city").toString() == user_location_city as String
        reader.parquetToObj(output).get(index).get("orig_destination_distance").toString() == orig_destination_distance as String
        reader.parquetToObj(output).get(index).get("user_id").toString() == user_id as String
        reader.parquetToObj(output).get(index).get("is_mobile").toString() == is_mobile as String
        reader.parquetToObj(output).get(index).get("is_package").toString() == is_package as String
        reader.parquetToObj(output).get(index).get("channel").toString() == channel as String
        reader.parquetToObj(output).get(index).get("srch_ci").toString() == srch_ci as String
        reader.parquetToObj(output).get(index).get("srch_co").toString() == srch_co as String
        reader.parquetToObj(output).get(index).get("srch_adults_cnt").toString() == srch_adults_cnt as String
        reader.parquetToObj(output).get(index).get("srch_children_cnt").toString() == srch_children_cnt as String
        reader.parquetToObj(output).get(index).get("srch_rm_cnt").toString() == srch_rm_cnt as String
        reader.parquetToObj(output).get(index).get("srch_destination_id").toString() == srch_destination_id as String
        reader.parquetToObj(output).get(index).get("srch_destination_type_id").toString() == srch_destination_type_id as String
        reader.parquetToObj(output).get(index).get("hotel_continent").toString() == hotel_continent as String
        reader.parquetToObj(output).get(index).get("hotel_country").toString() == hotel_country as String
        reader.parquetToObj(output).get(index).get("hotel_market").toString() == hotel_market as String


        where:
        index | date_time             | site_name | posa_continent | user_location_country | user_location_region | user_location_city | orig_destination_distance | user_id | is_mobile | is_package | channel | srch_ci      | srch_co      | srch_adults_cnt | srch_children_cnt | srch_rm_cnt | srch_destination_id | srch_destination_type_id | hotel_continent | hotel_country | hotel_market
        0     | "2015-09-03 17:09:54" | 2         | 3              | 66                    | 174                  | 37449              | "5539.0567"               | 1       | 1         | 0          | 3       | "2016-05-19" | "2016-05-23" | 2               | 0                 | 1           | 12243               | 6                        | 6               | 204           | 27
        1     | "2015-08-10 13:35:02" | 11        | 3              | 214                   | 120                  | 44496              | ""                        | 56      | 0         | 0          | 10      | "2015-08-18" | "2015-08-21" | 2               | 1                 | 1           | 20813               | 6                        | 6               | 70            | 312

    }


    def "create parquet Train record"() {

        given:
        ParquetDataWorker<Train> reader = new ParquetDataWorker<>()

        String baseDir = getBaseDir()
        String input = baseDir + File.separator + "train.csv"
        output = File.createTempFile("csv-", ".parquet")
        Schema schema = Train.getClassSchema()


        when:
        ParquetDataWorker.csvToParquet(input, output.getAbsolutePath(), schema)

        then:
        noExceptionThrown()
        reader.parquetToObj(output).size() == 2
        reader.parquetToObj(output).get(index).get("date_time").toString() == date_time as String
        reader.parquetToObj(output).get(index).get("site_name").toString() == site_name as String
        reader.parquetToObj(output).get(index).get("posa_continent").toString() == posa_continent as String
        reader.parquetToObj(output).get(index).get("user_location_country").toString() == user_location_country as String
        reader.parquetToObj(output).get(index).get("user_location_region").toString() == user_location_region as String
        reader.parquetToObj(output).get(index).get("user_location_city").toString() == user_location_city as String
        reader.parquetToObj(output).get(index).get("orig_destination_distance").toString() == orig_destination_distance as String
        reader.parquetToObj(output).get(index).get("user_id").toString() == user_id as String
        reader.parquetToObj(output).get(index).get("is_mobile").toString() == is_mobile as String
        reader.parquetToObj(output).get(index).get("is_package").toString() == is_package as String
        reader.parquetToObj(output).get(index).get("channel").toString() == channel as String
        reader.parquetToObj(output).get(index).get("srch_ci").toString() == srch_ci as String
        reader.parquetToObj(output).get(index).get("srch_co").toString() == srch_co as String
        reader.parquetToObj(output).get(index).get("srch_adults_cnt").toString() == srch_adults_cnt as String
        reader.parquetToObj(output).get(index).get("srch_children_cnt").toString() == srch_children_cnt as String
        reader.parquetToObj(output).get(index).get("srch_rm_cnt").toString() == srch_rm_cnt as String
        reader.parquetToObj(output).get(index).get("srch_destination_id").toString() == srch_destination_id as String
        reader.parquetToObj(output).get(index).get("srch_destination_type_id").toString() == srch_destination_type_id as String
        reader.parquetToObj(output).get(index).get("is_booking").toString() == is_booking as String
        reader.parquetToObj(output).get(index).get("cnt").toString() == cnt as String
        reader.parquetToObj(output).get(index).get("hotel_country").toString() == hotel_country as String
        reader.parquetToObj(output).get(index).get("hotel_continent").toString() == hotel_continent as String
        reader.parquetToObj(output).get(index).get("hotel_country").toString() == hotel_country as String
        reader.parquetToObj(output).get(index).get("hotel_market").toString() == hotel_market as String
        reader.parquetToObj(output).get(index).get("hotel_cluster").toString() == hotel_cluster as String


        where:
        index | date_time             | site_name | posa_continent | user_location_country | user_location_region | user_location_city | orig_destination_distance | user_id | is_mobile | is_package | channel | srch_ci      | srch_co      | srch_adults_cnt | srch_children_cnt | srch_rm_cnt | srch_destination_id | srch_destination_type_id | is_booking | cnt | hotel_continent | hotel_country | hotel_market | hotel_cluster
        0     | "2014-08-11 07:46:59" | 2         | 3              | 66                    | 348                  | 48862              | "2234.2641"               | 12      | 0         | 1          | 9       | "2014-08-27" | "2014-08-31" | 2               | 0                 | 1           | 8250                | 1                        | 0          | 3   | 2               | 50            | 628          | 1
        1     | "2014-08-11 08:22:12" | 2         | 3              | 66                    | 348                  | 48862              | ""                        | 12      | 0         | 1          | 9       | "2014-08-29" | "2014-09-02" | 2               | 0                 | 1           | 8250                | 1                        | 1          | 1   | 2               | 50            | 628          | 1
    }


    def "create SampleSubmission record"() {

        given:
        ParquetDataWorker<SampleSubmission> reader = new ParquetDataWorker<>()

        String baseDir = getBaseDir()
        String input = baseDir + File.separator + "sample_submission.csv"
        output = File.createTempFile("csv-", ".parquet")
        Schema schema = SampleSubmission.getClassSchema()


        when:
        ParquetDataWorker.csvToParquet(input, output.getAbsolutePath(), schema)

        then:
        noExceptionThrown()
        reader.parquetToObj(output).size() == 9
        reader.parquetToObj(output).get(0).getAt("id") == 0

    }




    private def getBaseDir() {
        def res = getClass().getClassLoader().getResource("csv")
        def file = Paths.get(res.toURI()).toFile()
        file.getAbsolutePath()
    }

}

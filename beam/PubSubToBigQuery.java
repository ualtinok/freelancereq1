import org.apache.beam.examples.common.WriteOneFilePerWindow;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.joda.time.Duration;

import java.io.IOException;


public class PubSubToBigQuery {
  /*
  * Define your own configuration options. Add your own arguments to be processed
  * by the command-line parser, and specify default values for them.
  */
  public interface PubSubToBigQueryOptions extends PipelineOptions, StreamingOptions {

    @Description("The Cloud Pub/Sub subscription to read from.")
    @Required
    String getInputSubscription();
    void setInputSubscription(String value);
  }

  public static void main(String[] args) throws IOException {

    PubSubToBigQueryOptions options = PipelineOptionsFactory
      .fromArgs(args)
      .withValidation()
      .as(PubSubToBigQueryOptions.class);

    options.setStreaming(true);

    Pipeline pipeline = Pipeline.create(options);

    pipeline
      .apply("Read PubSub Messages", PubsubIO.readAvros().fromSubscription(options.getInputSubscription()))
//      .apply(
//              ParDo.of(new DoFn<AvroReader>() {
//                @ProcessElement
//                public void processElement(ProcessContext c) {
//
//                  c.output(c.element());
//                }
//              })
//      )
      .apply(BigQueryIO.writeTableRows()
              .to("tablename")
              .withSchema());


    pipeline.run().waitUntilFinish();
  }
}

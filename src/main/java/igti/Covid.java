package igti;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Iterator;

public class Covid {

    private static Logger logger = Logger.getLogger(Covid.class);
    public static class IGTIMapper extends Mapper
            <LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key,
                           Text value,
                           Context context)
                throws IOException, InterruptedException {

            String[] dadosCovid = value.toString().split(",");
            String data = dadosCovid[0];
            String pais = dadosCovid[2];
             int novosCasos = 0;
             int novosObtidos =  0;

             try {
                 novosCasos = Integer.parseInt(dadosCovid[4]);
                 novosObtidos = Integer.parseInt(dadosCovid[6]);
             } catch (NumberFormatException e) {
                 novosCasos = Integer.parseInt(dadosCovid[5]);
                 novosObtidos = Integer.parseInt(dadosCovid[7]);
             }

            Text txtChave = new Text(data);
            Text txtValue = new Text(pais + "|" + novosCasos + "|" + novosObtidos);
//            System.out.println(txtChave.toString() + " " + value.toString());
            context.write(txtChave, txtValue);
        }
    }


    public static class IGTIReducer extends Reducer
            <Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key,
                              Iterable<Text> values,
                              Context context)
                throws IOException, InterruptedException {


            int maiorCasos = 0;
            int maiorObitos = 0;
            String paisCasos = "";
            String paisObitos = "";
            String[] campos;
            String saida = "";

            final Iterator<Text> iterator = values.iterator();
            while (iterator.hasNext()) {
//                logger.info(value.toString());
                campos = iterator.next()
                        .toString()
                        .split("\\|");


                if(campos.length > 1) {
                    int mc = Integer.parseInt(campos[1]);

                    if (mc > maiorCasos) {
                        logger.info(mc);
                        maiorCasos = mc;
                        paisCasos = campos[0];
                    }

                    int mo = Integer.parseInt(campos[2]);
                    if (mo > maiorObitos) {
                        logger.info(mo);
                        maiorObitos = mo;
                        paisObitos = campos[0];
                    }
                }

            }

            Text s = new Text();
            saida =  maiorCasos + "|" + paisCasos + "|";
            saida +=  maiorObitos + "|" + paisObitos;
            s.set(saida);

            logger.info(saida);
            IntWritable i = new IntWritable();
            i.set(maiorCasos);
            context.write(key, s);
        }
    }


    public static void main(String[] args) throws IOException,
            ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        final Job job = Job.getInstance(conf, "Covid19");
        FileSystem fs = FileSystem.get(conf);
        Path saida = new Path(args[1]);
        if(fs.exists(saida)) {
            logger.info("Excluindo output");
            fs.delete(saida, true);
        }
        job.setJarByClass(Covid.class);
        job.setMapperClass(IGTIMapper.class);
        job.setReducerClass(IGTIReducer.class);
//        job.setCombinerClass(IGTIReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, saida);
        job.waitForCompletion(true);
//        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}

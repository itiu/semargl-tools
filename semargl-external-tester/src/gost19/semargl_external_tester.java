package gost19;

import gost19.amqp.messaging.AMQPMessagingManager;
import gost19.amqp.messaging.MessageParser;
import gost19.amqp.messaging.TripleUtils;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 *
 * @author itiu
 */
public class semargl_external_tester
{

    public String autotest_file = "io_messages.log";
    public long count_repeat = 1;
    public boolean nocompare = false;
    public boolean is_reply_to_null = false;
    public String queue = "semargl-test";
    public AMQPMessagingManager mm;
    public long count = 0;
    public long start_time;
    public long stop_time;
    public long count_bytes;
    public int delta = 1000;
    private TripleUtils tripleUtils = new TripleUtils();
    private MessageParser messageParser = new MessageParser();
    String host = "192.168.0.101"; //"itiu-desktop";
    Integer port = 5672;
    String virtualHost = "bigarchive";
    String userName = "ba";
    String password = "123456";
    long responceWaitingLimit = 10000;

    semargl_external_tester() throws Exception
    {
    }

    public void init() throws Exception
    {
        mm = new AMQPMessagingManager();

        mm.init(host, port, virtualHost, userName, password, responceWaitingLimit);
    }

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws Exception
    {
        System.out.println("Semargl external tester v 0.0.1");

        semargl_external_tester autotest = new semargl_external_tester();

        if (args.length > 0)
        {
            for (int i = 0; i < args.length; i++)
            {
                if (args[i].equals("-autotest") || args[i].equals("-a"))
                {
                    System.out.println("autotest mode");
                    autotest.autotest_file = args[i + 1];
                    System.out.println("autotest file = " + autotest.autotest_file);
                }
                if (args[i].equals("-repeat") || args[i].equals("-r"))
                {
                    autotest.count_repeat = Long.parseLong(args[i + 1]);
                    System.out.println("repeat = " + autotest.count_repeat);
                }
                if (args[i].equals("-rt0"))
                {
                    autotest.is_reply_to_null = true;
                    System.out.println("mode: reply_to is null");
                }
                if (args[i].equals("-nocompare") || args[i].equals("-n"))
                {
                    autotest.nocompare = true;
                    System.out.println("mode: no compare");
                }
                if (args[i].equals("-message_service"))
                {
                    autotest.host = args[i + 1];
                    System.out.println("message_service : " + autotest.host);
                }
            }
        }

        autotest.init();


        autotest.start_time = System.currentTimeMillis();
        autotest.count_bytes = 0;

        for (int i = 0; i < autotest.count_repeat; i++)
        {
            autotest.load_messages_from_file();
        }

        System.exit(0);
    }

    private void load_messages_from_file() throws Exception
    {
        try
        {
            // Open the file that is the first
            // command line parameter
            FileInputStream fstream = new FileInputStream(autotest_file);
            // Get the object of DataInputStream
            DataInputStream in = new DataInputStream(fstream);
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            String strLine;

            //Read File Line By Line

            String input_message_et = null;
            List<String> output_message_et = null;

            while ((strLine = br.readLine()) != null)
            {
                if (strLine.indexOf(" INPUT") > 0)
                {
                    input_message_et = br.readLine();
                    output_message_et = new ArrayList<String>();
                }

                if (strLine.indexOf(" OUTPUT") > 0)
                {
                    String msg = br.readLine();
                    output_message_et.add(msg);

                    if (msg.indexOf(Predicates.STATE_OK) > 0)
                    {
                        go(input_message_et, output_message_et);
                    }
                }

            }
            //Close the input stream
            in.close();
        } catch (Exception e)
        {//Catch exception if any
            e.printStackTrace();
            System.err.println("Error: " + e.getMessage());
        }
    }

    void go(String input_message_et, List<String> output_message_et) throws Exception
    {
        String reply_to;
        String templ = Predicates.REPLY_TO + ">";
        int str_pos_beg = input_message_et.indexOf(templ);
        if (str_pos_beg <= 0)
        {
            throw new Exception("reply_to not found");
        }

        str_pos_beg += templ.length();
        str_pos_beg = input_message_et.indexOf("\"", str_pos_beg);
        int str_pos_end = input_message_et.indexOf("\"", str_pos_beg + 1);

        reply_to = input_message_et.substring(str_pos_beg + 1, str_pos_end);

        String new_reply_to = "";

        if (is_reply_to_null == false)
        {
            new_reply_to = "me_" + java.util.UUID.randomUUID().toString();
//                        new_reply_to = mm.allocateQueue();
        }


        input_message_et = input_message_et.replaceAll(templ + "\"" + reply_to, templ + "\"" + new_reply_to);
        reply_to = new_reply_to;

        templ = Predicates.CREATE;
        if (input_message_et.indexOf(Predicates.CREATE) > 0)
        {
            String newId = null;
            str_pos_beg = output_message_et.indexOf(Predicates.RESULT_DATA);
            str_pos_beg += Predicates.RESULT_DATA.length() + 1;

            str_pos_end = output_message_et.get(0).indexOf(".", str_pos_beg);

            newId = output_message_et.get(0).substring(str_pos_beg + 1, str_pos_end - 1);

            input_message_et = input_message_et.replaceAll("<>", "<" + newId + ">");
//                        System.out.println("create: [" + newId + "]");
//                        System.out.println("str_input_message_et = " + str_input_message_et);

        }

//                et_result.addAll(tripleUtils.getDataFromReply(output_message_et.toString()));
        List<String> get_result = null;
        List<String> eth_result = new ArrayList<String>();


        for (String element : output_message_et)
        {
            eth_result.addAll(tripleUtils.getDataFromReply(element));
        }
//                    System.out.println("\r\r\r\r\r");
//                    System.out.println("INPUT:");
//                    System.out.println(str_input_message_et);
//                    System.out.println("OUTPUT:");
//                    System.out.println(output_message_et);

//                    System.out.println("reply_to = " + reply_to);

        count++;

        try
        {

            if (nocompare == true)
            {
                mm.sendMessage(queue, input_message_et, reply_to);
            } else
            {
                List<String> etTriples = messageParser.split(input_message_et);
                String op_uid = tripleUtils.getTripleFromLine(etTriples.get(0)).getSubj();
                get_result = mm.sendRequest(op_uid, queue, input_message_et, false, reply_to);
                count_bytes += input_message_et.length();


                if (eth_result != null && get_result != null)
                {
                    if (eth_result.size() != get_result.size())
                    {
                        System.out.println("et_result.size() != result.size()");
                        return;
                    }

                    List<String> eth_elements = new ArrayList<String>();
                    List<String> get_elements = new ArrayList<String>();

                    // соберем ответные пакеты в общий массив
                    for (int i = 0; i < eth_result.size(); i++)
                    {
                        String[] arrayOf_S_Et;
                        String[] arrayOf_S_Get;

                        String S_Et = eth_result.get(i);
                        String S_Get = get_result.get(i);

                        if (S_Et.indexOf(".") > 0)
                        {
                            arrayOf_S_Et = S_Et.split("\"[.]");
                            arrayOf_S_Get = S_Get.split("\"[.]");

                        } else
                        {
                            arrayOf_S_Et = S_Et.split(",");
                            arrayOf_S_Get = S_Get.split(",");
                        }

                        eth_elements.addAll(Arrays.asList(arrayOf_S_Et));
                        get_elements.addAll(Arrays.asList(arrayOf_S_Get));
                    }

                    if (eth_elements.size() != get_elements.size())
                    {
                        throw new Exception("count elements not equals: ethalon.length=" + eth_elements.size() + ", result.length=" + get_elements.size());
                    }


                    for (int ii = 0; ii < eth_elements.size(); ii++)
                    {
                        boolean ii_found = false;
                        String ethalon = eth_elements.get(ii);

                        for (int jj = 0; jj < get_elements.size(); jj++)
                        {
                            if (ethalon.equals(get_elements.get(jj)))
                            {
                                //                                                        System.out.println("[" + arrayOf_S_Et[ii] + "], ii=" + ii + ", jj=" + jj);
                                ii_found = true;
                                break;
                            }
                        }

                        if (ii_found == false)
                        {
                            throw new Exception("ethalon element [" + ethalon + "] not found in result");
                        }
                    }







                    if (count % 1000 == 0)
                    {
                        System.out.println("sleep 10s");
                        Thread.currentThread().sleep(10000);
                    }

                    if (count % delta == 0)
                    {
                        stop_time = System.currentTimeMillis();

                        double time_in_sec = ((double) (stop_time - start_time)) / 1000;

                        System.out.println("send " + count + " messages, time = " + time_in_sec + ", cps = " + delta / time_in_sec + ", count kBytes = " + count_bytes / 1024);

                        //                        Thread.currentThread().sleep(100);

                        start_time = System.currentTimeMillis();
                        count_bytes = 0;
                    }


                }
                else
                    throw new Exception ("eth_result == null OR get_result == null");
            }

        } catch (Exception ex)
        {
            System.out.println("\r\r\r\r\r");
            System.out.println("INPUT:");
            System.out.println(input_message_et);
            System.out.println("OUTPUT:");
            System.out.println(output_message_et);

            throw ex;
        }




    }
}

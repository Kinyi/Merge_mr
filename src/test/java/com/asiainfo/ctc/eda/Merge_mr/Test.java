package com.asiainfo.ctc.eda.Merge_mr;

import static org.junit.Assert.*;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;


public class Test {

	@org.junit.Test
	public void DateTest() throws Exception {

		String dString = "2015-12-10 19:20:00";

		long timestamp_trans = timestamp_trans(dString);

		// System.out.println(Math.max(1, 1));
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd  HH:mm:ss");

		System.out.println(sdf.format(new Date(0)));

		// System.out.println(timestamp_trans);
		// System.out.println(format(timestamp_trans));
	}

	@org.junit.Test
	public void MRTest() throws Exception {
		String centence = "13311361915|59|2015-10-30 15:44:21|2015-10-30 15:50:20|8098|106854|102|0|10.56.0.145";
		String[] split = centence.split("\\|");

		for (int i = 0; i < 10; i++) {

		}
	}

	@org.junit.Test
	public void sortTest() throws Exception {
		ArrayList<String> list = new ArrayList<String>();

		list.add("13311361915;59;2015-10-30 15:40:21;2015-10-30 15:59:20;18098;136854;152;0;10.56.0.145");
		list.add("13311361915;59;2015-10-30 13:17:27;2015-10-30 14:11:41;872979;2599118;3391;0;10.56.0.145");
		list.add("13311361915;59;2015-10-30 18:44:21;2015-10-30 18:50:20;1098;6854;82;0;10.56.0.145");
		list.add("13311361915;59;2015-10-30 13:38:22;2015-10-30 13:44:19;10825;24953;35;0;10.56.0.145");
		list.add("13311361915;59;2015-10-30 13:16:27;2015-10-30 13:18:41;2;3;4;0;10.56.0.145");

		Collections.sort(list, new Comparator<String>() {

			public int compare(String o1, String o2) {
				SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

				Date date1 = null;
				Date date2 = null;
				try {
					date1 = sdf.parse(o1.substring(15, 34));
					date2 = sdf.parse(o2.substring(15, 34));
				} catch (ParseException e) {
					e.printStackTrace();
				}
				return date1.compareTo(date2);
			}

		});

		for (String string : list) {
			System.out.println(string);
		}
	}

	@org.junit.Test
	public void subTest() throws Exception {
		String str = "13311361915;59;2015-10-30 15:40:21;2015-10-30 15:59:20;18098;136854;152;0;10.56.0.145";

		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

		System.out.println(sdf.parse(str.substring(15, 34)).getTime());

	}

	@org.junit.Test
	public void IterableTest() throws Exception {
		ArrayList<String> list = new ArrayList<String>();
		list.add("kinyi");
		list.add("allen");
		list.add("bryant");
		Iterator<String> iterator = list.iterator();

		int i = 0;
		while (iterator.hasNext()) {
			i++;
			System.out.println(i);
			String aString = iterator.next();
			Iterator<String> iterator2 = list.iterator();
			while (iterator2.hasNext()) {
				// System.out.println(iterator2.next());
				String bString = iterator2.next();
				System.out.println(aString + ":" + bString);
			}
			// System.out.println(iterator.next());
		}

		/*
		 * for (String string : list) { System.out.println(string); }
		 */

	}

	@org.junit.Test
	public void SetTest() throws Exception {
		HashSet<String> hashSet = new HashSet<String>();
		hashSet.add("kinyi");
		hashSet.add("allen");

		System.out.println(hashSet.contains("kinyi"));
	}

	static long timestamp_trans(String input) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date parse = null;
		try {
			parse = sdf.parse(input);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return parse.getTime();
	}

	static String format(long timestamp) {
		Date date = new Date(timestamp);
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		String format = sdf.format(date);
		return format;
	}
	
	@org.junit.Test
	public void timeInstance() throws Exception {
		long t1 = 1446183859000L;
		long t2 = 1446183556000L;
		
		System.out.println(format(t1));
		System.out.println(format(t2));
	}

	@org.junit.Test
	public void reduceTest() throws Exception {
		List<String> out_list = new ArrayList<String>();
		List<String> in_list = new ArrayList<String>();

		out_list.add("13311361915;59;2015-10-30 13:44:21;2015-10-30 13:44:26;11985;150621;159;0;10.56.0.145");
		out_list.add("13311361915;59;2015-10-30 13:17:27;2015-10-30 14:11:41;872979;2599118;3391;0;10.56.0.145");
		out_list.add("13311361915;59;2015-10-30 13:16:27;2015-10-30 13:18:41;2;3;4;0;10.56.0.145");
		out_list.add("13311361915;59;2015-10-30 13:38:42;2015-10-30 13:39:16;2476;2016;5;0;10.56.0.145");
		out_list.add("13311361915;59;2015-10-30 13:38:22;2015-10-30 13:44:19;10825;24953;35;0;10.56.0.145");

		in_list.add("13311361915;59;2015-10-30 13:44:21;2015-10-30 13:44:26;11985;150621;159;0;10.56.0.145");    
		in_list.add("13311361915;59;2015-10-30 13:17:27;2015-10-30 14:11:41;872979;2599118;3391;0;10.56.0.145"); 
		in_list.add("13311361915;59;2015-10-30 13:16:27;2015-10-30 13:18:41;2;3;4;0;10.56.0.145");               
		in_list.add("13311361915;59;2015-10-30 13:38:42;2015-10-30 13:39:16;2476;2016;5;0;10.56.0.145");         
		in_list.add("13311361915;59;2015-10-30 13:38:22;2015-10-30 13:44:19;10825;24953;35;0;10.56.0.145");      

		// 对存储value值的两个list进行排序(按起始时间)
		Collections.sort(out_list, new Comparator<String>() {

			public int compare(String o1, String o2) {
				SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

				Date date1 = null;
				Date date2 = null;
				try {
					date1 = sdf.parse(o1.substring(15, 34));
					date2 = sdf.parse(o2.substring(15, 34));
				} catch (ParseException e) {
					e.printStackTrace();
				}
				return date1.compareTo(date2);
			}
		});

		Collections.sort(in_list, new Comparator<String>() {

			public int compare(String o1, String o2) {
				SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

				Date date1 = null;
				Date date2 = null;
				try {
					date1 = sdf.parse(o1.substring(15, 34));
					date2 = sdf.parse(o2.substring(15, 34));
				} catch (ParseException e) {
					e.printStackTrace();
				}
				return date1.compareTo(date2);
			}
		});

		// 合并后的记录
		Set<String> merged_set = new HashSet<String>();
		// 合并过的记录
		Set<String> repeat_set = new HashSet<String>();

		// 日期格式化相关
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");


		for (String whole_out : out_list) {

			if (!repeat_set.contains(whole_out)) {

				String[] split_out = whole_out.split(";");

				long start_out = 0L;
				long end_out = 0L;
				long flux_out = 0L;
				try {
					start_out = sdf.parse(split_out[2]).getTime();
					end_out = sdf.parse(split_out[3]).getTime();
					flux_out = Long.parseLong(split_out[6]);
				} catch (ParseException e) {
					e.printStackTrace();
				}

				// 合并后的流量
				long flux_total = flux_out;
				// 合并后的起始时间
				long start_merged = start_out;
				// 合并后的结束时间
				long end_merged = end_out;


				for (String whole_in : in_list) {


					String[] split_in = whole_in.split(";");

					long start_in = 0L;
					long end_in = 0L;
					long flux_in = 0L;
					try {
						start_in = sdf.parse(split_in[2]).getTime();
						end_in = sdf.parse(split_in[3]).getTime();
						flux_in = Long.parseLong(split_in[6]);
					} catch (ParseException e) {
						e.printStackTrace();
					}

					// 不是自己本身的那条记录
					if (!whole_in.equals(whole_out)) {
						// 两条记录相互比较
						if (start_merged >= start_in && start_merged <= end_in
								|| start_in >= start_merged && start_in <= end_merged) {
							flux_total = flux_total + flux_in;
							start_merged = Math.min(start_in, start_merged);
							end_merged = Math.max(end_in, end_merged);
							// 把合并过的记录保存到repeat_set
							repeat_set.add(whole_in);
						}
					}
				}
				merged_set.add(split_out[0] + "\t" + sdf.format(new Date(start_merged)) + "\t"
						+ sdf.format(new Date(end_merged)) + "\t" + flux_total + "\t" + split_out[8]);
			} else {
				continue;
			}
		}

		for (String record : merged_set) {
			System.out.println(record);
		}
	}
}

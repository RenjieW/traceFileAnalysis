import numpy as np
import os
import argparse
import linecache

def numpy_array_generator(trace_file_path, start_num):
    '''
    format:
    op_type, relative timestamp
    
    '''
    first_line = linecache.getline(trace_file_path, start_num)
    first_arr = first_line.split()
    first_time = int(first_arr[10])
    first_utime = int(first_arr[11])

    op_arr = []
    timestamp_arr = []
    with open(trace_file_path, 'r') as filehandler:
        curr_num = 0;
        for line in filehandler:
            curr_num += 1
            if (curr_num < start_num):
                continue

            try:
                arr_line = line.split()
                op_type = int(arr_line[7].split('-')[1])
                timestamp = int(arr_line[10]) - first_time + (int(arr_line[11])- first_utime) / 1000000

                op_arr.append(op_type)
                timestamp_arr.append(timestamp)
            except:
                continue


    op_arr = np.array(op_arr).astype(int)
    timestamp_arr = np.array(timestamp_arr).astype(float)
    info_arr = np.array([op_arr, timestamp_arr])
    print(info_arr.shape)
    return info_arr



def aver_phase_time(trace_file_path, start_num, init_op):
    info_arr = numpy_array_generator(trace_file_path, int(line))
    info_len = info_arr.shape[1]

    iter_count = 0
    curr_init_op = init_op
    last_pull_recv_index = np.where(info_arr[0][:] == (curr_init_op-1))[0][-1]

    comp = 0
    pure_comm = 0
    overlap = 0
    iter_time = 0
    i = 0

    while (True):
        prev_iter_index = last_pull_recv_index
        push_send = np.where(info_arr[0][:] == curr_init_op)[0]
        if push_send.size == 0:
            break

        first_push_send_index = push_send[0]
        last_push_send_index = push_send[-1]
        last_pull_recv_index = np.where(info_arr[0][:] == (curr_init_op+3))[0][-1]
        # print(prev_iter_index, first_push_send_index, last_push_send_index, last_pull_recv_index)

        iter_count += 1
        comp += info_arr[1][last_push_send_index] - info_arr[1][prev_iter_index]
        iter_time += info_arr[1][last_pull_recv_index] - info_arr[1][prev_iter_index]
        pure_comm += info_arr[1][last_pull_recv_index] - info_arr[1][last_push_send_index]
        overlap += info_arr[1][last_push_send_index] - info_arr[1][first_push_send_index]

        curr_init_op += 4


    aver_comp = comp / iter_count
    aver_iter_time = iter_time / iter_count
    aver_overlap = overlap / iter_count
    print(aver_comp, aver_iter_time, aver_overlap/aver_iter_time)




if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='read trace files')
    parser.add_argument('--file', metavar='<file name>',
                        help='file to read', required=True)
    parser.add_argument('--dir', metavar='<dir name>',
                        help='dir which stores files', required=True)
    parser.add_argument('--line', metavar='<start line>',
                        help='line start to read', required=True)
    parser.add_argument('--op', metavar='<initial op>',
                        help='initial operation be analyzed', required=True)

    args = parser.parse_args()
    file_name = args.file
    dir_name = args.dir
    line = args.line
    op = args.op

    prefix = dir_name.split('/')[1]
    src_dir = os.path.join('.', dir_name, 'data_trace')
    dst_dir = os.path.join('.', dir_name.split('/')[0], 'numpy_files')

    trace_file_path = os.path.join(src_dir, file_name)
    numpy_file_path = os.path.join(dst_dir, prefix+'.npy')

    # print(trace_file_path)
    # print(numpy_file_path)


    aver_phase_time(trace_file_path, int(line), int(op))
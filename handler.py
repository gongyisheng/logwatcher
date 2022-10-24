class MsgHandler(object):
    def __init__(self, msg_pattern, funcs):
        self.msg_pattern = msg_pattern # message pattern
        self.funcs = funcs # a list of functions to handle the message
        
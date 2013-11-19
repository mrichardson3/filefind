import time

class Timer():
    """ Quickie little class that let's you wrap a function call
        and get the amount of time it burned executing. This was a
        very small class that quickly ballooned due to pylint's
        complaints about it.  Well, it's dumb, but pylint finally approves.
    """
    def __init__(self):
        """Really a dummy function to hold the starting time for when needed"""
        self.start = None

    def __enter__(self):
        """Start the stop watch by getting the current time"""
        self.start_timer()

    def __exit__(self, *args):
        """Stop the stop watch by subtracting the current time from the
           time when the stop watch was started.  Print out the total
           amount of execution time.
        """
        self.stop_timer()

    def start_timer(self):
        """get the current time"""
        self.start = time.time()

    def stop_timer(self):
        """compute total amount of time since stop watch was started"""
        print("Total Execution time is -> " + str(time.time() - self.start))


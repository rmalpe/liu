from datetime import datetime, timedelta
from typing import List, Optional

from liualgotrader.common.data_loader import DataLoader
from liualgotrader.common.tlog import tlog
from liualgotrader.scanners.base import Scanner


class GoldDigger(Scanner):
    name = "GoldDigger"

    def __init__(
        self,
        data_loader: DataLoader,
        recurrence: Optional[timedelta] = None,
        target_strategy_name: str = None,
    ):
        super().__init__(
            name=self.name,
            data_loader=data_loader,
            recurrence=recurrence,
            target_strategy_name=target_strategy_name,
        )

    async def run(self) -> List[str]:
        try:
            return (
                ["PNB","SBIN","NTPC"]
            )
        except Exception as e:
            print(e)